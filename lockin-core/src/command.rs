use crate::node::{Metadata, NodeId};
use serde::{Deserialize, Serialize};

///Command represnets write requests for namespace sent by client ,
///which leader will replicate on other lokcin nodes
#[derive(Serialize, Deserialize)]
pub enum Command {
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
/// Query represents namespace read queries sent by client,
/// Queries are handled by leader node only , queries are not replicated on other nodes
pub enum Query {
    GetContents { id: NodeId },
}

pub enum QueryResponse {
    GetContentsOk {
        data: bytes::Bytes,
        metadata: Metadata,
    },
    Error(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CommandResponse {
    OpenOk { node_id: NodeId, metadata: Metadata },
    MkdirOk { node_id: NodeId, metadata: Metadata },
    SetContentsOk { metadata: Metadata },
    DeleteOK,
}
