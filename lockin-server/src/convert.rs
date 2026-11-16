use bytes::Bytes;
use lockin_core::command::{Command, CommandResponse, NamespaceCommand, SessionStoreCommand};
use lockin_core::node::{Metadata, NodeId};
use lockin_core::session::SessionId;
use lockin_proto::message::{NamespaceResponse, NamespaceWriteRequest, SessionResponse};

/// Wire namespace write → Raft `Command`.
pub fn command_from_write(r: NamespaceWriteRequest) -> Command {
    let inner = match r {
        NamespaceWriteRequest::Open { path, create } => NamespaceCommand::Open { path, create },
        NamespaceWriteRequest::Mkdir { path } => NamespaceCommand::Mkdir { path },
        NamespaceWriteRequest::SetContents {
            id,
            expected_gen,
            data,
        } => NamespaceCommand::SetContents {
            id: NodeId::new(id),
            expected_gen,
            data,
        },
        NamespaceWriteRequest::Delete { id } => NamespaceCommand::Delete {
            id: NodeId::new(id),
        },
    };
    Command::NamespaceCommandType(inner)
}

pub fn command_create_session() -> Command {
    Command::SessionStoreCommandType(SessionStoreCommand::CreateSession)
}

pub fn command_expire_session(session_id: SessionId) -> Command {
    Command::SessionStoreCommandType(SessionStoreCommand::ExpireSession { session_id })
}

/// Raft apply result → namespace client response.
pub fn namespace_response_from_command(r: CommandResponse) -> anyhow::Result<NamespaceResponse> {
    match r {
        CommandResponse::OpenOk { node_id, metadata } => Ok(NamespaceResponse::OpenOk {
            id: node_id.as_u64(),
            instance: metadata.instance,
            content_gen: metadata.content_gen,
        }),
        CommandResponse::MkdirOk { node_id, metadata } => Ok(NamespaceResponse::MkdirOk {
            id: node_id.as_u64(),
            instance: metadata.instance,
            content_gen: metadata.content_gen,
        }),
        CommandResponse::SetContentsOk { metadata } => Ok(NamespaceResponse::SetContentsOk {
            instance: metadata.instance,
            content_gen: metadata.content_gen,
        }),
        CommandResponse::DeleteOK => Ok(NamespaceResponse::DeleteOk),
        CommandResponse::CreateSessionOk { .. } | CommandResponse::ExpireSessionOk => {
            anyhow::bail!("not a namespace CommandResponse")
        }
    }
}

pub fn namespace_response_from_query(data: Bytes, metadata: Metadata) -> NamespaceResponse {
    NamespaceResponse::GetContentsOk {
        data: data,
        content_gen: metadata.content_gen,
        instance: metadata.instance,
    }
}

/// SM `CreateSessionOk` + leader lease TTL → wire session response.
pub fn session_create_ok_from_response(
    r: CommandResponse,
    lease_timeout_ms: u64,
) -> anyhow::Result<SessionResponse> {
    match r {
        CommandResponse::CreateSessionOk { session_id } => Ok(SessionResponse::CreateOk {
            session_id: session_id.as_u64(),
            lease_timeout_ms,
        }),
        _ => anyhow::bail!("not a CreateSessionOk CommandResponse"),
    }
}

pub fn session_expire_ok_from_response(r: CommandResponse) -> anyhow::Result<SessionResponse> {
    match r {
        CommandResponse::ExpireSessionOk => Ok(SessionResponse::KeepAliveExpired),
        _ => anyhow::bail!("not an ExpireSessionOk CommandResponse"),
    }
}
