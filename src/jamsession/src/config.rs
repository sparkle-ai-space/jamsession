use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use agent_client_protocol::schema::{EnvVariable, McpServer, McpServerStdio};
use agent_client_protocol::{AcpAgent, Client, DynConnectTo};
use serde::Deserialize;

use crate::agent::AgentFactory;
use crate::error::Error;

#[derive(Debug, Deserialize, Default)]
pub struct Config {
    pub daemon: Option<DaemonConfig>,
    pub agent: Option<AgentConfig>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct DaemonConfig {
    pub log_filter: Option<String>,
    pub idle_timeout_secs: Option<u64>,
    pub quiescence_timeout_secs: Option<u64>,
    #[serde(alias = "default-model")]
    pub default_model: Option<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    pub name: Option<String>,
    pub custom: Option<CustomAgent>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CustomAgent {
    pub path: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

impl Config {
    pub fn log_filter(&self) -> Option<&str> {
        self.daemon.as_ref()?.log_filter.as_deref()
    }

    pub fn default_model(&self) -> Option<&str> {
        self.daemon.as_ref()?.default_model.as_deref()
    }

    pub fn idle_timeout(&self) -> std::time::Duration {
        let secs = self.daemon.as_ref().and_then(|d| d.idle_timeout_secs).unwrap_or(900);
        std::time::Duration::from_secs(secs)
    }

    pub fn quiescence_timeout(&self) -> std::time::Duration {
        let secs = self.daemon.as_ref().and_then(|d| d.quiescence_timeout_secs).unwrap_or(10);
        std::time::Duration::from_secs(secs)
    }

    pub fn daemon_env(&self) -> impl Iterator<Item = (&str, &str)> {
        self.daemon
            .iter()
            .flat_map(|d| d.env.iter())
            .map(|(k, v)| (k.as_str(), v.as_str()))
    }

    pub fn load(config_dir: &Path) -> Self {
        let path = config_dir.join("config.toml");
        let contents = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => {
                tracing::info!(path = %path.display(), "no config.toml found, using defaults");
                return Self::default();
            }
        };
        match toml::from_str(&contents) {
            Ok(config) => {
                let config: Self = config;
                tracing::info!(path = %path.display(), ?config, "loaded config");
                config
            }
            Err(e) => {
                tracing::warn!(path = %path.display(), "invalid config.toml, using defaults: {e}");
                Self::default()
            }
        }
    }

    pub fn into_factory(self) -> Arc<dyn AgentFactory> {
        match self.agent {
            Some(AgentConfig {
                custom: Some(custom),
                ..
            }) => {
                tracing::info!(
                    path = %custom.path.display(),
                    args = ?custom.args,
                    env = ?custom.env,
                    "using custom agent factory"
                );
                Arc::new(CustomAgentFactory { custom })
            }
            Some(AgentConfig {
                name: Some(name), ..
            }) => {
                tracing::info!(name = %name, "using acpr agent factory");
                Arc::new(crate::agent::AcprFactory::new(name))
            }
            _ => {
                tracing::info!("using default acpr agent factory (claude-acp)");
                Arc::new(crate::agent::AcprFactory::default())
            }
        }
    }
}

struct CustomAgentFactory {
    custom: CustomAgent,
}

impl AgentFactory for CustomAgentFactory {
    fn create_transport(
        &self,
        session_id: &str,
        _cwd: &Path,
        _mcp_servers: &[McpServer],
    ) -> Result<DynConnectTo<Client>, Error> {
        let env: Vec<EnvVariable> = self
            .custom
            .env
            .iter()
            .map(|(k, v)| EnvVariable::new(k.clone(), v.clone()))
            .collect();

        tracing::info!(
            session_id,
            command = %self.custom.path.display(),
            args = ?self.custom.args,
            env_count = env.len(),
            "spawning custom agent"
        );

        let server = McpServerStdio::new("custom-agent", &self.custom.path)
            .args(self.custom.args.clone())
            .env(env);

        let agent = AcpAgent::new(McpServer::Stdio(server));
        Ok(DynConnectTo::new(agent))
    }
}
