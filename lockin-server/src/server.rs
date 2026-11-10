use anyhow::Context;
use lockin_core::{
    command::{CommandResponse, Query},
    node::NodeId,
    state_machine::LockinStateMachine,
};
use lockin_proto::message::{
    ErrorCode, LockinRequest, LockinResponse, Message, NamespaceReadRequest, NamespaceRequest,
    NamespaceResponse, NamespaceWriteRequest,
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

    /// orchastrates response flow to the client
    async fn write_namespace(
        &self,
        conn_id: ConnectionId,
        write_req: NamespaceWriteRequest,
    ) -> anyhow::Result<()> {
        let ns_response = self.try_namespace_write(write_req).await;

        let msg = match ns_response {
            Ok(response) => Message::Response(LockinResponse::Namespace(response)),
            Err(e) => match self.raft_node.is_leader() {
                true => Message::Response(LockinResponse::Error {
                    code: ErrorCode::Internal,
                    message: e.to_string(),
                }),
                false => Message::Response(LockinResponse::NotLeader { leader_hint: None }),
            },
        };

        self.to_net
            .send(ServerToNet { conn_id, msg })
            .await
            .context("failed to send response to client")
    }

    /// serializes write request , proposes write request to other nodes using raft, serialize raft
    /// response into NamespaceResponse and return it
    async fn try_namespace_write(
        &self,
        write_req: NamespaceWriteRequest,
    ) -> anyhow::Result<NamespaceResponse> {
        let cmd = command_from_write(write_req);
        let cmd_bytes = bincode_next::serde::encode_to_vec(&cmd, bincode_next::config::standard())
            .context("failed to serialize command")?;

        match self.raft_node.propose(cmd_bytes).await {
            Ok(response) => {
                let (cmd_resp, _): (CommandResponse, usize) =
                    bincode_next::serde::decode_from_slice(
                        &response,
                        bincode_next::config::standard(),
                    )
                    .context("failed to decode CommandResponse from raft")?;

                Ok(namespace_response_from_command(cmd_resp))
            }
            Err(e) => Err(e),
        }
    }

    async fn read_namespace(
        &self,
        id: ConnectionId,
        read_req: NamespaceReadRequest,
    ) -> anyhow::Result<()> {
        let response_result = self.try_namespace_read(read_req).await;
        let msg = match response_result {
            Ok(response) => Message::Response(LockinResponse::Namespace(response)),
            Err(e) => Message::Response(LockinResponse::Error {
                code: ErrorCode::Internal,
                message: e.to_string(),
            }),
        };
        self.to_net
            .send(ServerToNet {
                conn_id: id,
                msg: msg,
            })
            .await
            .context("failed to send response to client")
    }

    /// parse read request , read state , return namespace response
    async fn try_namespace_read(
        &self,
        read_req: NamespaceReadRequest,
    ) -> anyhow::Result<NamespaceResponse> {
        let parsed_req = match read_req {
            NamespaceReadRequest::GetContents { id } => Query::GetContents {
                id: NodeId::new(id),
            },
        };

        let query_response = self
            .state_machine
            .read_state(parsed_req)
            .context("failed to read namespace state")?;
        Ok(namespace_response_from_query(query_response))
    }
}
