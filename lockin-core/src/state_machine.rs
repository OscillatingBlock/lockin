use anyhow::Context;
use bytes::Bytes;
use mini_raft::state_machine::StateMachine;
use std::sync::Mutex;

use crate::{
    command::Command,
    node::{Metadata, NodeId},
    session::{SessionId, SessionStore},
    tree::Namespace,
};

pub struct LockinStateMachine {
    namespace: Mutex<Namespace>,
    session_store: Mutex<SessionStore>,
}

impl LockinStateMachine {
    pub fn new() -> Self {
        Self {
            namespace: Mutex::new(Namespace::new()),
            session_store: Mutex::new(SessionStore::new()),
        }
    }

    pub fn apply_logs(&self, command: &bytes::Bytes) -> anyhow::Result<Bytes> {
        self.apply_log(command)
    }

    pub fn get_content(&self, id: NodeId) -> anyhow::Result<(Bytes, Metadata)> {
        self.namespace
            .lock()
            .unwrap()
            .get_contents(id)
            .context("failed to get content")
    }

    pub fn session_exists(&self, id: &SessionId) -> bool {
        self.session_store.lock().unwrap().contains(id)
    }
}

impl StateMachine for LockinStateMachine {
    fn apply_log(&self, command: &bytes::Bytes) -> anyhow::Result<bytes::Bytes> {
        let config = bincode_next::config::standard();
        let (decoded_cmd, _): (Command, usize) =
            bincode_next::serde::decode_from_slice(command, config)
                .context("failed to decode command into NamespaceCommand")?;

        let response = match decoded_cmd {
            Command::NamespaceCommandType(ns_cmd) => {
                self.namespace
                    .lock()
                    .unwrap()
                    .apply(ns_cmd)
                    .context("failed to apply namespace command")?
            }
            Command::SessionStoreCommandType(sess_cmd) => self
                .session_store
                .lock()
                .unwrap()
                .apply(sess_cmd)
                .context("failed to apply session store command")?,
        };

        let out = bincode_next::serde::encode_to_vec(&response, config)?;
        Ok(Bytes::from(out))
    }
}
