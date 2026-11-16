use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use lockin_core::{
    command::CommandResponse, node::NodeId, session::SessionId, state_machine::LockinStateMachine,
};
use lockin_proto::message::{
    ErrorCode, LockinRequest, LockinResponse, Message, NamespaceReadRequest, NamespaceRequest,
    NamespaceResponse, NamespaceWriteRequest, SessionRequest, SessionResponse,
};
use mini_raft::server::RaftNode;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::{
    convert::{
        command_create_session, command_expire_session, command_from_write,
        namespace_response_from_command, namespace_response_from_query,
        session_create_ok_from_response, session_expire_ok_from_response,
    },
    lease_store::LeaderLeaseTable,
    net::{ConnectionId, NetToServer, ServerToNet},
};

pub struct LockinServer {
    state_machine: Arc<LockinStateMachine>,
    raft_node: Arc<dyn RaftNodeTrait>,
    to_net: mpsc::Sender<ServerToNet>,
    lease_store: LeaderLeaseTable,
}

#[async_trait]
pub trait RaftNodeTrait: Send + Sync + 'static {
    async fn propose(&self, command: bytes::Bytes) -> anyhow::Result<Bytes>;
    fn is_leader(&self) -> bool;
}

#[async_trait]
impl RaftNodeTrait for RaftNode {
    fn is_leader(&self) -> bool {
        self.is_leader()
    }
    async fn propose(&self, command: bytes::Bytes) -> anyhow::Result<Bytes> {
        self.propose(command).await
    }
}

impl LockinServer {
    pub fn new(
        state_machine: Arc<LockinStateMachine>,
        raft_node: Arc<dyn RaftNodeTrait>,
        to_net: mpsc::Sender<ServerToNet>,
    ) -> Self {
        Self {
            state_machine,
            raft_node,
            to_net,
            lease_store: LeaderLeaseTable::new(12000, 4000),
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

            LockinRequest::Session(sess) => match sess {
                SessionRequest::Create => self
                    .create_session(id)
                    .await
                    .context("failed to create session"),
                SessionRequest::KeepAlive { session_id } => self
                    .keep_alive(id, session_id)
                    .await
                    .context("failed to handle keep alive request"),
                SessionRequest::Close { session_id } => self
                    .close_session(id, session_id)
                    .await
                    .context("failed to close session"),
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

        match self.raft_node.propose(cmd_bytes.into()).await {
            Ok(slice) => {
                let (cmd_resp, _): (CommandResponse, usize) =
                    bincode_next::serde::decode_from_slice(
                        &slice,
                        bincode_next::config::standard(),
                    )
                    .context("failed to decode CommandResponse from raft")?;
                let Ok(ns_res) = namespace_response_from_command(cmd_resp) else {
                    anyhow::bail!("expected namespace response");
                };
                Ok(ns_res)
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
        let node_id = match read_req {
            NamespaceReadRequest::GetContents { id } => NodeId::new(id),
        };

        let (bytes, metadata) = self
            .state_machine
            .get_content(node_id)
            .context("failed to read namespace state")?;
        Ok(namespace_response_from_query(bytes, metadata))
    }

    async fn create_session(&self, conn_id: ConnectionId) -> anyhow::Result<()> {
        let msg = match self.try_create_session().await {
            Ok(sess_res) => Message::Response(LockinResponse::Session(sess_res)),
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

    async fn try_create_session(&self) -> anyhow::Result<SessionResponse> {
        if !self.raft_node.is_leader() {
            anyhow::bail!("not leader")
        }
        let cmd = command_create_session();
        let cmd_bytes = bincode_next::serde::encode_to_vec(&cmd, bincode_next::config::standard())
            .context("failed to serialize command")?;

        match self.raft_node.propose(Bytes::from(cmd_bytes)).await {
            Ok(slice) => {
                let (cmd_resp, _): (CommandResponse, usize) =
                    bincode_next::serde::decode_from_slice(
                        &slice,
                        bincode_next::config::standard(),
                    )
                    .context("failed to decode CommandResponse from raft")?;

                let CommandResponse::CreateSessionOk { session_id } = &cmd_resp else {
                    anyhow::bail!("exptected CreateSessionOK, failed to create session")
                };
                let lease_timeout_ms = self.lease_store.renew_lease(&session_id);
                session_create_ok_from_response(cmd_resp, lease_timeout_ms)
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    async fn keep_alive(&self, conn_id: ConnectionId, session_id: u64) -> anyhow::Result<()> {
        let msg = match self.try_keep_alive(SessionId::from_u64(session_id)).await {
            Ok(msg) => Message::Response(LockinResponse::Session(msg)),
            Err(e) => Message::Response(LockinResponse::Error {
                code: ErrorCode::Internal,
                message: e.to_string(),
            }),
        };
        self.to_net
            .send(ServerToNet { conn_id, msg })
            .await
            .context("failed to send response to client")
    }

    async fn try_keep_alive(&self, session_id: SessionId) -> anyhow::Result<SessionResponse> {
        if !self.raft_node.is_leader() {
            anyhow::bail!("not leader");
        }

        if !self.state_machine.session_exists(&session_id) {
            warn!("session not present in session store, expired session");
            return Ok(SessionResponse::KeepAliveExpired);
        }

        debug!("calling expires in ");
        let expires_at =
            Instant::now() + Duration::from_millis(self.lease_store.expires_in_ms(&session_id));
        debug!("expires in expires_in_ms");
        if Instant::now() >= expires_at {
            debug!("expired session");
            let msg = self.propose_expire_session(&session_id).await;
            self.lease_store
                .remove(&session_id)
                .context("failed to remove lease from lease store")?;
            return msg;
        } else {
            debug!("sleeping");
            tokio::time::sleep_until(tokio::time::Instant::from_std(
                expires_at - Duration::from_millis(self.lease_store.slack_duration_ms),
            ))
            .await;
        }

        let new_expires_at = self.lease_store.renew_lease(&session_id);
        Ok(SessionResponse::KeepAliveOk {
            session_id: session_id.as_u64(),
            lease_timeout_ms: new_expires_at,
        })
    }

    async fn propose_expire_session(
        &self,
        session_id: &SessionId,
    ) -> anyhow::Result<SessionResponse> {
        let cmd = command_expire_session(session_id.clone());
        let cmd_bytes = bincode_next::serde::encode_to_vec(&cmd, bincode_next::config::standard())
            .context("failed to serialize command")?;

        match self.raft_node.propose(Bytes::from(cmd_bytes)).await {
            Ok(slice) => {
                let (cmd_resp, _): (CommandResponse, usize) =
                    bincode_next::serde::decode_from_slice(
                        &slice,
                        bincode_next::config::standard(),
                    )
                    .context("failed to decode CommandResponse from raft")?;
                session_expire_ok_from_response(cmd_resp)
            }
            Err(e) => Err(e),
        }
    }

    async fn close_session(&self, conn_id: ConnectionId, session_id: u64) -> anyhow::Result<()> {
        let response = self
            .try_close_session(SessionId::from_u64(session_id))
            .await;
        let msg = match response {
            Ok(msg) => Message::Response(LockinResponse::Session(msg)),
            Err(e) => Message::Response(LockinResponse::Error {
                code: ErrorCode::Internal,
                message: e.to_string(),
            }),
        };
        self.to_net
            .send(ServerToNet { conn_id, msg })
            .await
            .context("failed to send response to client")
    }

    async fn try_close_session(&self, session_id: SessionId) -> anyhow::Result<SessionResponse> {
        self.propose_expire_session(&session_id).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use tracing::debug;

    use super::*;

    struct MockRaftNode {
        state_machine: Arc<LockinStateMachine>,
        is_leader: AtomicBool,
    }

    impl MockRaftNode {
        fn new(state_machine: Arc<LockinStateMachine>, is_leader: bool) -> Self {
            Self {
                state_machine,
                is_leader: AtomicBool::new(is_leader),
            }
        }
    }

    #[async_trait]
    impl RaftNodeTrait for MockRaftNode {
        fn is_leader(&self) -> bool {
            self.is_leader.load(Ordering::SeqCst)
        }

        async fn propose(&self, command: bytes::Bytes) -> anyhow::Result<Bytes> {
            if self.is_leader.load(Ordering::SeqCst) {
                self.state_machine.apply_logs(&command)
            } else {
                anyhow::bail!("not leader");
            }
        }
    }

    impl MockRaftNode {
        fn set_leader(&self, state: bool) {
            self.is_leader.store(state, Ordering::SeqCst);
        }
    }

    /// Single unified test initializer taking an `is_leader` flag
    fn init_test(is_leader: bool) -> (LockinServer, mpsc::Receiver<ServerToNet>) {
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();

        let state_machine = Arc::new(LockinStateMachine::new());
        let (to_net, from_server) = mpsc::channel::<ServerToNet>(100);
        let raft_node = MockRaftNode::new(Arc::clone(&state_machine), is_leader);

        let server = LockinServer::new(state_machine, Arc::new(raft_node), to_net);
        (server, from_server)
    }

    fn init_test_mut(
        is_leader: bool,
    ) -> (LockinServer, mpsc::Receiver<ServerToNet>, Arc<MockRaftNode>) {
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();

        let state_machine = Arc::new(LockinStateMachine::new());
        let (to_net, from_server) = mpsc::channel::<ServerToNet>(100);
        let raft_node = MockRaftNode::new(Arc::clone(&state_machine), is_leader);
        let raft_node_clone = Arc::new(raft_node);

        let server = LockinServer::new(state_machine, raft_node_clone.clone(), to_net);
        (server, from_server, raft_node_clone.clone())
    }

    /// Helper to quickly set up an open file for tests that need an existing ID
    async fn open_test_file(server: &LockinServer, path: &str) -> NamespaceResponse {
        let req = NamespaceWriteRequest::Open {
            path: String::from(path),
            create: true,
        };
        server.try_namespace_write(req).await.unwrap()
    }

    async fn open_test_dir(server: &LockinServer, path: &str) -> NamespaceResponse {
        let req = NamespaceWriteRequest::Mkdir {
            path: String::from(path),
        };
        server.try_namespace_write(req).await.unwrap()
    }

    async fn write_file(
        server: &LockinServer,
        id: u64,
        expected_gen: u64,
        data: Bytes,
    ) -> NamespaceResponse {
        let req = NamespaceWriteRequest::SetContents {
            id: id,
            expected_gen: expected_gen,
            data: data,
        };
        println!("writing contents to file");
        server.try_namespace_write(req).await.unwrap()
    }

    #[tokio::test]
    async fn try_write_open() {
        let (server, _) = init_test(true);
        let write_req = NamespaceWriteRequest::Open {
            path: String::from("/W"),
            create: true,
        };

        let response = server.try_namespace_write(write_req).await.unwrap();

        assert_eq!(
            response,
            NamespaceResponse::OpenOk {
                id: 1,
                content_gen: 0,
                instance: 1
            }
        );
    }

    #[tokio::test]
    async fn try_write_mkdir() {
        let (server, _) = init_test(true);
        let write_req = NamespaceWriteRequest::Mkdir {
            path: String::from("/six"),
        };

        let response = server.try_namespace_write(write_req).await.unwrap();

        assert_eq!(
            response,
            NamespaceResponse::MkdirOk {
                id: 1,
                content_gen: 0,
                instance: 1
            }
        );
    }

    #[tokio::test]
    async fn try_write_set_content_ok() {
        let (server, _) = init_test(true);

        // Setup initial file
        let _ = open_test_file(&server, "/W").await;

        let set_content_req = NamespaceWriteRequest::SetContents {
            id: 1,
            expected_gen: 0,
            data: Bytes::from_static(b"new content"),
        };

        let response = server.try_namespace_write(set_content_req).await.unwrap();

        assert_eq!(
            response,
            NamespaceResponse::SetContentsOk {
                content_gen: 1,
                instance: 1
            }
        );
    }

    #[tokio::test]
    async fn test_try_write_set_cas_fail() {
        let (server, _) = init_test(true);
        let _ = open_test_file(&server, "/W").await;

        // First write succeeds (gen 0 -> 1)
        let first_write = NamespaceWriteRequest::SetContents {
            id: 1,
            expected_gen: 0,
            data: Bytes::from_static(b"new content"),
        };
        server.try_namespace_write(first_write).await.unwrap();

        // Second write with stale expected_gen (0 instead of 1) should fail CAS
        let stale_write = NamespaceWriteRequest::SetContents {
            id: 1,
            expected_gen: 0,
            data: Bytes::from_static(b"six seven"),
        };

        let response = server.try_namespace_write(stale_write).await;
        assert!(response.is_err(), "expected CAS failure error");
    }

    #[tokio::test]
    async fn test_try_write_delete() {
        let (server, _) = init_test(true);
        let _ = open_test_file(&server, "/W").await;

        let delete_req = NamespaceWriteRequest::Delete { id: 1 };
        let response = server.try_namespace_write(delete_req).await.unwrap();

        assert_eq!(response, NamespaceResponse::DeleteOk);
    }

    #[tokio::test]
    async fn test_try_write_propose_fails() {
        let (server, _) = init_test(false); // Not leader
        let write_req = NamespaceWriteRequest::Open {
            path: String::from("/W"),
            create: true,
        };

        let response = server.try_namespace_write(write_req).await;
        assert!(response.is_err(), "expected proposal error when not leader");
    }

    #[tokio::test]
    async fn test_try_read_after_set() {
        let (server, _) = init_test(true);
        open_test_file(&server, "/W").await;
        write_file(&server, 1, 0, Bytes::from_static(b"data")).await;

        let read_req = NamespaceReadRequest::GetContents { id: 1 };
        let response = server.try_namespace_read(read_req).await.unwrap();
        let NamespaceResponse::GetContentsOk {
            data,
            content_gen,
            instance,
        } = &response
        else {
            panic!("expected GetContentsOk type")
        };
        assert_eq!(data, &Bytes::from_static(b"data"));
        assert_eq!(*content_gen, 1);
        assert_eq!(*instance, 1);
    }

    #[tokio::test]
    async fn test_try_read_missing() {
        let (server, _) = init_test(true);
        let read_req = NamespaceReadRequest::GetContents { id: 1 };
        let response = server.try_namespace_read(read_req).await;
        let Err(_) = response else {
            panic!("expected error");
        };
    }

    #[tokio::test]
    async fn test_try_read_directory() {
        let (server, _) = init_test(true);
        open_test_dir(&server, "/dir/").await;
        let read_req = NamespaceReadRequest::GetContents { id: 1 };
        let response = server.try_namespace_read(read_req).await;
        let Err(_) = response else {
            panic!("expected error");
        };
    }

    #[tokio::test]
    async fn test_try_read_empty_file() {
        let (server, _) = init_test(true);
        open_test_file(&server, "/W").await;

        let read_req = NamespaceReadRequest::GetContents { id: 1 };
        let response = server.try_namespace_read(read_req).await.unwrap();
        let NamespaceResponse::GetContentsOk {
            data,
            content_gen,
            instance,
        } = &response
        else {
            panic!("expected GetContentsOk type")
        };
        assert_eq!(data, &Bytes::from_static(b""));
        assert_eq!(*content_gen, 0);
        assert_eq!(*instance, 1);
    }

    #[tokio::test]
    async fn test_write_sends_namespace_ok() {
        let (server, mut rx) = init_test(true);
        let open_req = NamespaceWriteRequest::Open {
            path: String::from("/file"),
            create: true,
        };
        server.write_namespace(1, open_req).await.unwrap();

        let server_to_net = rx.recv().await.unwrap();
        assert_eq!(server_to_net.conn_id, 1);
        let Message::Response(response) = &server_to_net.msg else {
            panic!("expected response");
        };
        let LockinResponse::Namespace(ns_response) = response else {
            panic!("expected namespace response");
        };
        let NamespaceResponse::OpenOk {
            id,
            content_gen,
            instance,
        } = ns_response
        else {
            panic!("expected namespace response open ok");
        };
        assert_eq!(*id, 1);
        assert_eq!(*content_gen, 0);
        assert_eq!(*instance, 1);
    }

    #[tokio::test]
    async fn test_write_sends_not_leader() {
        let (server, mut rx) = init_test(false);
        let open_req = NamespaceWriteRequest::Open {
            path: String::from("/file"),
            create: true,
        };
        server.write_namespace(1, open_req).await.unwrap();

        let server_to_net = rx.recv().await.unwrap();
        assert_eq!(server_to_net.conn_id, 1);
        let Message::Response(response) = &server_to_net.msg else {
            panic!("expected response");
        };
        let LockinResponse::NotLeader { leader_hint: _ } = response else {
            panic!("expected Not Leader response");
        };
    }

    #[tokio::test]
    async fn test_read_sends_namespace_ok() {
        let (server, mut rx) = init_test(true);
        open_test_file(&server, "/W").await;
        write_file(&server, 1, 0, Bytes::from_static(b"data")).await;

        let read_req = NamespaceReadRequest::GetContents { id: 1 };
        server.read_namespace(1, read_req).await.unwrap();

        let server_msg = rx.recv().await.unwrap();
        let Message::Response(response) = &server_msg.msg else {
            panic!("expected response");
        };
        let LockinResponse::Namespace(ns_response) = response else {
            panic!("expected namespace response");
        };

        let NamespaceResponse::GetContentsOk {
            data,
            content_gen,
            instance,
        } = ns_response
        else {
            panic!("expected get content type response");
        };
        assert_eq!(*data, Bytes::from_static(b"data"));
        assert_eq!(*content_gen, 1);
        assert_eq!(*instance, 1);
    }

    #[tokio::test]
    async fn test_try_create_session() {
        let (server, _) = init_test(true);

        let res = server.try_create_session().await.unwrap();
        let SessionResponse::CreateOk {
            session_id,
            lease_timeout_ms,
        } = res
        else {
            panic!("expected no error");
        };

        assert_eq!(session_id, 0);
        assert!(lease_timeout_ms > server.lease_store.lease_duration_ms - 500);
    }

    #[tokio::test]
    async fn test_try_create_session_not_leader() {
        let (server, _) = init_test(false);

        let res = server.try_create_session().await;
        let Err(_) = res else {
            panic!("expected error")
        };
    }

    #[tokio::test]
    async fn test_try_create_two_session_distinct_id() {
        let (server, _) = init_test(true);

        let sess1 = server.try_create_session().await.unwrap();
        let sess2 = server.try_create_session().await.unwrap();
        let SessionResponse::CreateOk {
            session_id: session_id_1,
            lease_timeout_ms: _,
        } = sess1
        else {
            panic!("exptected CreateOk type")
        };
        let SessionResponse::CreateOk {
            session_id: session_id_2,
            lease_timeout_ms: _,
        } = sess2
        else {
            panic!("exptected CreateOk type")
        };
        assert_ne!(session_id_1, session_id_2);
    }

    #[tokio::test]
    async fn test_try_keep_alive_ok() {
        let (server, _) = init_test(true);

        let sess = server.try_create_session().await.unwrap();

        let SessionResponse::CreateOk {
            session_id,
            lease_timeout_ms: _,
        } = sess
        else {
            panic!("exptected CreateOk type")
        };

        let lease = server
            .try_keep_alive(SessionId::from_u64(session_id))
            .await
            .unwrap();

        let SessionResponse::KeepAliveOk {
            session_id: keep_alive_sess_id,
            lease_timeout_ms,
        } = lease
        else {
            panic!("exptected KeepAliveOk");
        };

        assert_eq!(session_id, keep_alive_sess_id);
        assert!(lease_timeout_ms > server.lease_store.lease_duration_ms - 500);
    }

    #[tokio::test]
    async fn test_try_keep_alive_unknown_session() {
        let (server, _) = init_test(true);
        let response = server
            .try_keep_alive(SessionId::from_u64(67))
            .await
            .unwrap();
        let SessionResponse::KeepAliveExpired = response else {
            panic!("expected KeepAliveExpired");
        };
    }

    #[tokio::test]
    async fn test_try_keep_alive_after_close() {
        let (server, _) = init_test(true);

        let sess = server.try_create_session().await.unwrap();
        let SessionResponse::CreateOk {
            session_id,
            lease_timeout_ms: _,
        } = sess
        else {
            panic!("exptected CreateOk type")
        };

        let sess_id = SessionId::from_u64(session_id);
        server.try_close_session(sess_id.clone()).await.unwrap();

        let response = server.try_keep_alive(sess_id).await.unwrap();
        let SessionResponse::KeepAliveExpired = response else {
            panic!("expected KeepAliveExpired");
        };
    }

    #[tokio::test]
    async fn test_try_keep_alive_after_expire() {
        let (server, _) = init_test(true);

        let sess = server.try_create_session().await.unwrap();
        let SessionResponse::CreateOk {
            session_id,
            lease_timeout_ms: _,
        } = sess
        else {
            panic!("exptected CreateOk type")
        };

        let sess_id = SessionId::from_u64(session_id);
        server.propose_expire_session(&sess_id).await.unwrap();

        let response = server.try_keep_alive(sess_id).await.unwrap();
        let SessionResponse::KeepAliveExpired = response else {
            panic!("expected KeepAliveExpired");
        };
    }

    #[tokio::test]
    async fn test_try_keep_alive_not_leader() {
        let (server, _, raft_node) = init_test_mut(true);

        let sess = server.try_create_session().await.unwrap();
        let SessionResponse::CreateOk {
            session_id,
            lease_timeout_ms: _,
        } = sess
        else {
            panic!("exptected CreateOk type")
        };

        raft_node.set_leader(false);
        let res = server.try_keep_alive(SessionId::from_u64(session_id)).await;
        let Err(_) = res else {
            panic!("expected error")
        };
    }

    #[tokio::test]
    async fn test_try_keep_alive_rearms_missing_lease() {
        let (server, _) = init_test(true);

        let sess = server.try_create_session().await.unwrap();
        let SessionResponse::CreateOk {
            session_id,
            lease_timeout_ms: _,
        } = sess
        else {
            panic!("exptected CreateOk type")
        };

        let sess_id = SessionId::from_u64(session_id);
        server.lease_store.remove(&sess_id).unwrap();

        debug!("calling keep alive");
        let response = server.try_keep_alive(sess_id.clone()).await.unwrap();
        let SessionResponse::KeepAliveOk {
            session_id,
            lease_timeout_ms,
        } = response
        else {
            panic!("exptected KeepAliveOk type")
        };

        assert_eq!(session_id, sess_id.as_u64());
        assert!(lease_timeout_ms > server.lease_store.lease_duration_ms - 500);
    }

    #[tokio::test]
    async fn test_expire_existing_session() {
        let (server, _) = init_test(true);

        let sess = server.try_create_session().await.unwrap();
        let SessionResponse::CreateOk {
            session_id,
            lease_timeout_ms: _,
        } = sess
        else {
            panic!("exptected CreateOk type")
        };

        let sid = SessionId::from_u64(session_id);
        let response = server.propose_expire_session(&sid).await.unwrap();

        let SessionResponse::KeepAliveExpired = response else {
            panic!("expected KeepAliveExpired type")
        };
    }

    #[tokio::test]
    //TODO:
    async fn test_expire_missing_session() {}
}
