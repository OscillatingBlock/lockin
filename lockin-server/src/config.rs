use anyhow::Context;
use config::{Environment, File};
use mini_raft::config::ServerConfig as RaftConfig;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    lockin_config: LockinConfig,
    raft_config: RaftConfig,
}

#[derive(Deserialize)]
struct LockinConfig {
    client_listener_address: String,
    node_id: u64,
}

impl Config {
    pub fn load() -> anyhow::Result<Self> {
        // Precedence: MINI_RAFT_CONFIG env var, else "config" (backwards compat).
        // CLI --config handling lives in main.rs, which calls load_from directly.
        let path = std::env::var("MINI_RAFT_CONFIG").unwrap_or_else(|_| "config".to_string());
        Self::load_from(&path)
    }

    pub fn load_from(path: &str) -> anyhow::Result<Self> {
        // Strip a trailing .toml so both "config" and "config-node1.toml" work.
        let stem = path.strip_suffix(".toml").unwrap_or(path);
        let config = config::Config::builder()
            .set_default("election_timeout", 500)?
            .set_default("heartbeat_interval", 200)?
            .add_source(
                File::with_name(stem)
                    .format(config::FileFormat::Toml)
                    .required(true),
            )
            .add_source(
                Environment::with_prefix("MINI_RAFT")
                    .separator("_")
                    .try_parsing(true),
            )
            .build()
            .context("Failed to build configuration builder")?;
        let app_config: Config = config
            .try_deserialize()
            .context("Failed to deserialize configuration into Config struct")?;

        Ok(app_config)
    }

    pub fn client_listener_address(&self) -> String {
        String::from(self.lockin_config.client_listener_address.clone())
    }

    pub fn raft_config(&self) -> RaftConfig {
        self.raft_config.clone()
    }
}
