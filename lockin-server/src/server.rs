use anyhow::Context;
use lockin_core::{
    command::{CommandResponse, Query},
    node::NodeId,
    state_machine::LockinStateMachine,
};
use lockin_proto::message::{
    ErrorCode, LockinRequest, LockinResponse, Message, NamespaceReadRequest, NamespaceRequest,
    NamespaceWriteRequest,
};
use mini_raft::server::RaftNode;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::warn;

use crate::{
    convert::{command_from_write, namespace_response_from_command, namespace_response_from_query},
    net::{ConnectionId, NetToServer, ServerToNet},
};

pub struct LockinServer {
    state_machine: Arc<LockinStateMachine>,
    raft_node: RaftNode,
    to_net: mpsc::Sender<ServerToNet>,
}

impl LockinServer {
    pub fn new(
        state_machine: LockinStateMachine,
        raft_node: RaftNode,
        to_net: mpsc::Sender<ServerToNet>,
    ) -> Self {
        Self {
            state_machine: Arc::new(state_machine),
            raft_node,
            to_net,
        }
    }

    pub async fn start(
        self: Arc<Self>,
        mut from_net: mpsc::Receiver<NetToServer>,
    ) -> anyhow::Result<()> {
        while let Some(msg) = from_net.recv().await {
            let self_clone = self.clone();
            tokio::spawn(async move {
                if let Err(e) = self_clone.handle_msg(msg.msg, msg.conn_id).await {
                    warn!("failure while handling client request: {e}");
                }
            });
        }
        Ok(())
    }

    async fn handle_msg(&self, msg: Message, id: ConnectionId) -> anyhow::Result<()> {
        let Message::Request(request) = msg else {
            warn!("server got response message instead of request");
            anyhow::bail!("server got response message instead of request");
        };

        match request {
            LockinRequest::Namespace(ns_req) => match ns_req {
                NamespaceRequest::Write(write_req) => self
                    .write_namespace(id, write_req)
                    .await
                    .context("failed to handle namespace write request"),
                NamespaceRequest::Read(ns_read_req) => self
                    .read_namespace(id, ns_read_req)
                    .await
                    .context("failed to handle namespace read request"),
            },
        }
    }

    async fn write_namespace(
        &self,
        conn_id: ConnectionId,
        write_req: NamespaceWriteRequest,
    ) -> anyhow::Result<()> {
        let cmd = command_from_write(write_req);
        let cmd_bytes = bincode_next::serde::encode_to_vec(&cmd, bincode_next::config::standard())
            .context("failed to serialize command")?;

        match self.raft_node.propose(cmd_bytes).await {
            Ok(response_bytes) => {
                // apply_log encoded a CommandResponse
                let (cmd_resp, _): (CommandResponse, usize) =
                    bincode_next::serde::decode_from_slice(
                        &response_bytes,
                        bincode_next::config::standard(),
                    )
                    .context("failed to decode CommandResponse from raft")?;

                let ns_res = namespace_response_from_command(cmd_resp);
                let msg = Message::Response(LockinResponse::Namespace(ns_res));

                self.to_net
                    .send(ServerToNet { conn_id, msg })
                    .await
                    .context("failed to send write response to client")
            }
            Err(e) => {
                let msg = match self.raft_node.is_leader() {
                    true => Message::Response(LockinResponse::Error {
                        code: ErrorCode::Internal,
                        message: e.to_string(),
                    }),
                    false => Message::Response(LockinResponse::NotLeader { leader_hint: None }),
                };
                self.to_net
                    .send(ServerToNet { conn_id, msg })
                    .await
                    .context("failed to send error response to client")
            }
        }
    }

    async fn read_namespace(
        &self,
        id: ConnectionId,
        read_req: NamespaceReadRequest,
    ) -> anyhow::Result<()> {
        let parsed_req = match read_req {
            NamespaceReadRequest::GetContents { id } => Query::GetContents {
                id: NodeId::new(id),
            },
        };

        let query_response = self
            .state_machine
            .read_state(parsed_req)
            .context("failed to read state")?;
        let ns_res = namespace_response_from_query(query_response);
        let msg = LockinResponse::Namespace(ns_res);
        self.to_net
            .send(ServerToNet {
                conn_id: id,
                msg: Message::Response(msg),
            })
            .await
            .context("failed to send response to client")
    }
}
