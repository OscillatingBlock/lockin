use anyhow::Context;
use bytes::Bytes;
use mini_raft::state_machine::StateMachine;
use std::sync::Mutex;

use crate::{
    command::{Command, CommandResponse, Query, QueryResponse},
    tree::Namespace,
};

pub struct LockinStateMachine {
    inner: Mutex<Namespace>,
}

impl LockinStateMachine {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Namespace::new()),
        }
    }

    pub fn apply_logs(&self, command: &bytes::Bytes) -> anyhow::Result<Bytes> {
        self.apply_log(command)
    }

    pub fn read_state(&self, request: Query) -> anyhow::Result<QueryResponse> {
        let bytes = match request {
            Query::GetContents { id } => self
                .inner
                .lock()
                .unwrap()
                .get_contents(id)
                .context("failed to get content")?,
        };
        Ok(QueryResponse::GetContentsOk {
            data: bytes.0,
            metadata: bytes.1,
        })
    }
}

impl StateMachine for LockinStateMachine {
    fn apply_log(&self, command: &bytes::Bytes) -> anyhow::Result<bytes::Bytes> {
        let config = bincode_next::config::standard();
        let (decoded_cmd, _): (Command, usize) =
            bincode_next::serde::decode_from_slice(command, config)
                .context("failed to decode command into NamespaceCommand")?;
        let response = self.inner.lock().unwrap().apply(decoded_cmd)?;
        let out = bincode_next::serde::encode_to_vec(&response, config)?;

        Ok(Bytes::from(out))
    }
}
