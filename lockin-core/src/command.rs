use crate::{
    node::{Metadata, NodeId},
    session::SessionId,
};
use serde::{Deserialize, Serialize};

///Command represnets write requests for namespace sent by client ,
///which leader will replicate on other lokcin nodes
#[derive(Serialize, Deserialize)]
pub enum Command {
    NamespaceCommandType(NamespaceCommand),
    SessionStoreCommandType(SessionStoreCommand),
}

#[derive(Serialize, Deserialize)]
pub enum NamespaceCommand {
    Open {
        path: String,
        create: bool,
    },
    Mkdir {
        path: String,
    },
    SetContents {
        id: NodeId,
        expected_gen: u64,
        data: bytes::Bytes,
    },
    Delete {
        id: NodeId,
    },
}

#[derive(Serialize, Deserialize)]
pub enum SessionStoreCommand {
    CreateSession,
    ExpireSession { session_id: SessionId },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CommandResponse {
    OpenOk { node_id: NodeId, metadata: Metadata },
    MkdirOk { node_id: NodeId, metadata: Metadata },
    SetContentsOk { metadata: Metadata },
    DeleteOK,
    CreateSessionOk { session_id: SessionId },
    ExpireSessionOk,
}
