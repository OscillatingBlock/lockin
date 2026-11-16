use lockin_core::session::SessionId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{self, Duration, Instant};
use tracing::info;

pub struct LeaderLeaseTable {
    pub leases: Arc<Mutex<HashMap<SessionId, Instant>>>,
    pub lease_duration_ms: u64,
    pub slack_duration_ms: u64,
}

impl LeaderLeaseTable {
    pub fn new(lease_duration_ms: u64, slack_duration_ms: u64) -> Self {
        Self {
            leases: Arc::new(Mutex::new(HashMap::new())),
            lease_duration_ms,
            slack_duration_ms,
        }
    }

    /// renews lease if present, creates one if not already present , and returns relative duration
    /// (eg 500 millis from now) to tell how much time left till lease expires
    pub fn renew_lease(&self, session_id: &SessionId) -> u64 {
        let expires_at = Instant::now() + Duration::from_millis(self.lease_duration_ms);
        let mut guard = self.leases.lock().unwrap();
        let lease = guard.get_mut(session_id);
        match lease {
            Some(instant) => {
                *instant = expires_at;
            }
            None => {
                guard.insert(session_id.clone(), expires_at);
            }
        }
        expires_at
            .saturating_duration_since(std::time::Instant::now())
            .as_millis() as u64
    }

    pub fn expires_in_ms(&self, session_id: &SessionId) -> u64 {
        let instant = {
            let guard = self.leases.lock().unwrap();
            guard.get(session_id).copied()
        };

        match instant {
            Some(inst) => {
                let ms = inst
                    .saturating_duration_since(time::Instant::now())
                    .as_millis() as u64;

                info!("expires in {ms}",);
                ms
            }
            None => self.renew_lease(session_id),
        }
    }

    pub fn remove(&self, session_id: &SessionId) -> anyhow::Result<()> {
        match self.leases.lock().unwrap().remove(session_id) {
            Some(_) => Ok(()),
            None => anyhow::bail!("Lease does not exist for this session"),
        }
    }
}
