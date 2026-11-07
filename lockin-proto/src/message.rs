use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Request(LockinRequest),
    Response(LockinResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LockinRequest {
    Namespace(NamespaceRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NamespaceRequest {
    Write(NamespaceWriteRequest),
    Read(NamespaceReadRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NamespaceWriteRequest {
    Open {
        path: String,
        create: bool,
    },
    Mkdir {
        path: String,
    },
    SetContents {
        id: u64,
        expected_gen: u64,
        data: bytes::Bytes,
    },
    Delete {
        id: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NamespaceReadRequest {
    GetContents { id: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LockinResponse {
    Namespace(NamespaceResponse),
    NotLeader { leader_hint: Option<String> },
    Error { code: ErrorCode, message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NamespaceResponse {
    OpenOk {
        id: u64,
        content_gen: u64,
        instance: u64,
    },
    MkdirOk {
        id: u64,
        content_gen: u64,
        instance: u64,
    },
    SetContentsOk {
        content_gen: u64,
        instance: u64,
    },
    GetContentsOk {
        data: bytes::Bytes,
        content_gen: u64,
        instance: u64,
    },
    DeleteOk,
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ErrorCode {
    NotFound,
    NotFile,
    NotDirectory,
    CasMismatch,
    TooLarge,
    InvalidPath,
    CannotDeleteRoot,
    Internal,
}
