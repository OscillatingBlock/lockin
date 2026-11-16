use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use thiserror::Error;

use crate::command::{CommandResponse, SessionStoreCommand};

#[derive(Error, Debug, Serialize, Deserialize)]
pub enum SessionStoreError {
    #[error("Session not found")]
    SessionNotFound,
    #[error("Invalid Session store command")]
    InvalidCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct SessionId(u64);

impl SessionId {
    pub fn from_u64(id: u64) -> Self {
        Self(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0.clone()
    }
}

pub struct Session {
    pub id: SessionId,
}

pub struct SessionLease {
    pub session_id: SessionId,
    pub expires_at: Instant,
}

#[derive(Default)]
pub struct SessionStore {
    sessions: Arc<RwLock<BTreeMap<SessionId, Session>>>,
    next_session_id: AtomicU64,
}

impl SessionStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_new_session(&self) -> SessionId {
        let next_id = self
            .next_session_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let session_id = SessionId::from_u64(next_id);
        self.sessions.write().unwrap().insert(
            session_id.clone(),
            Session {
                id: session_id.clone(),
            },
        );
        session_id
    }

    pub fn expire_session(&self, id: SessionId) -> Option<Session> {
        self.sessions.write().unwrap().remove(&id)
    }

    pub fn contains(&self, id: &SessionId) -> bool {
        self.sessions.read().unwrap().contains_key(id)
    }

    pub fn apply(
        &self,
        command: SessionStoreCommand,
    ) -> Result<CommandResponse, SessionStoreError> {
        match command {
            SessionStoreCommand::CreateSession => {
                let id = self.create_new_session();
                Ok(CommandResponse::CreateSessionOk { session_id: id })
            }
            SessionStoreCommand::ExpireSession { session_id } => {
                match self.expire_session(session_id) {
                    Some(_) => Ok(CommandResponse::ExpireSessionOk),
                    None => Err(SessionStoreError::SessionNotFound),
                }
            }
        }
    }
}
