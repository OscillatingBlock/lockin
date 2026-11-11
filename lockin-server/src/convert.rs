use lockin_core::command::{Command, CommandResponse, Query, QueryResponse};
use lockin_core::node::NodeId;
use lockin_proto::message::{NamespaceReadRequest, NamespaceResponse, NamespaceWriteRequest};

pub fn command_from_write(r: NamespaceWriteRequest) -> Command {
    match r {
        NamespaceWriteRequest::Open { path, create } => Command::Open { path, create },
        NamespaceWriteRequest::Mkdir { path } => Command::Mkdir { path },
        NamespaceWriteRequest::SetContents {
            id,
            expected_gen,
            data,
        } => Command::SetContents {
            id: NodeId::new(id),
            expected_gen,
            data,
        },
        NamespaceWriteRequest::Delete { id } => Command::Delete {
            id: NodeId::new(id),
        },
    }
}

pub fn _query_from_read(r: NamespaceReadRequest) -> Query {
    match r {
        NamespaceReadRequest::GetContents { id } => Query::GetContents {
            id: NodeId::new(id),
        },
    }
}

pub fn namespace_response_from_command(r: CommandResponse) -> NamespaceResponse {
    match r {
        CommandResponse::OpenOk { node_id, metadata } => NamespaceResponse::OpenOk {
            id: node_id.as_u64(),
            instance: metadata.instance,
            content_gen: metadata.content_gen,
        },
        CommandResponse::MkdirOk { node_id, metadata } => NamespaceResponse::MkdirOk {
            id: node_id.as_u64(),
            instance: metadata.instance,
            content_gen: metadata.content_gen,
        },
        CommandResponse::SetContentsOk { metadata } => NamespaceResponse::SetContentsOk {
            instance: metadata.instance,
            content_gen: metadata.content_gen,
        },
        CommandResponse::DeleteOK => NamespaceResponse::DeleteOk,
    }
}

pub fn namespace_response_from_query(r: QueryResponse) -> NamespaceResponse {
    match r {
        QueryResponse::GetContentsOk { data, metadata } => NamespaceResponse::GetContentsOk {
            data,
            instance: metadata.instance,
            content_gen: metadata.content_gen,
        },
        QueryResponse::Error(e) => NamespaceResponse::Error { message: e },
    }
}
