use std::path::Path;
use std::str::FromStr;

use agent_client_protocol::schema::McpServer;
use agent_client_protocol::{AcpAgent, Client, DynConnectTo};

use crate::error::Error;

/// Factory for creating agent connections. The daemon calls this when
/// a session needs an agent (session/new, session/load with dead agent).
///
/// The factory returns a type-erased transport; the caller connects to it
/// via `client_cx.spawn_connection(...)`.
pub trait AgentFactory: Send + Sync + 'static {
    fn create_transport(
        &self,
        session_id: &str,
        cwd: &Path,
        mcp_servers: &[McpServer],
    ) -> Result<DynConnectTo<Client>, Error>;
}

/// Production factory: spawns agent via acpr registry.
pub(super) struct AcprFactory {
    name: String,
}

impl AcprFactory {
    fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Default for AcprFactory {
    fn default() -> Self {
        Self::new("claude-acp")
    }
}

impl AgentFactory for AcprFactory {
    fn create_transport(
        &self,
        _session_id: &str,
        _cwd: &Path,
        _mcp_servers: &[McpServer],
    ) -> Result<DynConnectTo<Client>, Error> {
        Ok(DynConnectTo::new(acpr::Acpr::new(&self.name)))
    }
}

/// Test/dev factory: spawns agent from a binary path.
pub struct BinaryFactory {
    path: std::path::PathBuf,
}

impl BinaryFactory {
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl AgentFactory for BinaryFactory {
    fn create_transport(
        &self,
        _session_id: &str,
        _cwd: &Path,
        _mcp_servers: &[McpServer],
    ) -> Result<DynConnectTo<Client>, Error> {
        let cmd = self.path.display().to_string();
        Ok(DynConnectTo::new(
            AcpAgent::from_str(&cmd).expect("valid agent command"),
        ))
    }
}
