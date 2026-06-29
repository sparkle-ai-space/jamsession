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
    pub agent: Option<AgentConfig>,
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
    pub fn load(config_dir: &Path) -> Self {
        let path = config_dir.join("config.toml");
        let contents = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => return Self::default(),
        };
        match toml::from_str(&contents) {
            Ok(config) => config,
            Err(e) => {
                tracing::warn!("invalid config.toml, using defaults: {e}");
                Self::default()
            }
        }
    }

    pub fn into_factory(self) -> Arc<dyn AgentFactory> {
        match self.agent {
            Some(AgentConfig {
                custom: Some(custom),
                ..
            }) => Arc::new(CustomAgentFactory { custom }),
            Some(AgentConfig {
                name: Some(name), ..
            }) => Arc::new(crate::agent::AcprFactory::new(name)),
            _ => Arc::new(crate::agent::AcprFactory::default()),
        }
    }
}

struct CustomAgentFactory {
    custom: CustomAgent,
}

impl AgentFactory for CustomAgentFactory {
    fn create_transport(
        &self,
        _session_id: &str,
        _cwd: &Path,
        _mcp_servers: &[McpServer],
    ) -> Result<DynConnectTo<Client>, Error> {
        let env: Vec<EnvVariable> = self
            .custom
            .env
            .iter()
            .map(|(k, v)| EnvVariable::new(k.clone(), v.clone()))
            .collect();

        let server = McpServerStdio::new("custom-agent", &self.custom.path)
            .args(self.custom.args.clone())
            .env(env);

        let agent = AcpAgent::new(McpServer::Stdio(server));
        Ok(DynConnectTo::new(agent))
    }
}
