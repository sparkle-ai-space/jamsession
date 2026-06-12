use std::path::Path;
use std::str::FromStr;

use agent_client_protocol::schema::{
    InitializeRequest, InitializeResponse, LoadSessionRequest, McpServer, NewSessionRequest,
    NewSessionResponse, ProtocolVersion, SessionId,
};
use agent_client_protocol::{AcpAgent, Client, ConnectionTo, DynConnectTo};

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
pub struct AcprFactory {
    name: String,
}

impl AcprFactory {
    pub fn new(name: impl Into<String>) -> Self {
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

pub struct AgentManager;

impl AgentManager {
    /// Probe agent capabilities by spawning a short-lived "temp agent" (FR-008).
    pub async fn get_capabilities(
        req: &InitializeRequest,
        factory: &dyn AgentFactory,
    ) -> Result<InitializeResponse, Error> {
        let agent_transport = factory.create_transport("", Path::new("/"), &[])?;
        let init_req = req.clone();

        let response = Client
            .builder()
            .name("jamsession-daemon-caps")
            .connect_with(
                agent_transport,
                async move |cx: ConnectionTo<agent_client_protocol::Agent>| {
                    let resp = cx
                        .send_request(
                            InitializeRequest::new(ProtocolVersion::V1)
                                .client_capabilities(init_req.client_capabilities.clone()),
                        )
                        .block_task()
                        .await?;
                    Ok(resp)
                },
            )
            .await
            .map_err(|e| Error::AgentSpawn(format!("capabilities probe failed: {e}")))?;

        Ok(response)
    }

    /// Spawn an agent subprocess and return the connection handle.
    pub fn spawn_agent_connection(
        client_cx: &ConnectionTo<agent_client_protocol::Client>,
        factory: &dyn AgentFactory,
        session_id: &str,
        cwd: &Path,
        mcp_servers: &[McpServer],
    ) -> Result<ConnectionTo<agent_client_protocol::Agent>, Error> {
        let agent_transport = factory.create_transport(session_id, cwd, mcp_servers)?;
        client_cx
            .spawn_connection(
                Client.builder().name("jamsession-daemon-agent"),
                agent_transport,
            )
            .map_err(|e| Error::AgentSpawn(format!("failed to spawn agent connection: {e}")))
    }

    /// Initialize the ACP protocol on an agent connection.
    pub async fn initialize_agent(
        agent_cx: &ConnectionTo<agent_client_protocol::Agent>,
    ) -> Result<InitializeResponse, Error> {
        agent_cx
            .send_request(InitializeRequest::new(ProtocolVersion::V1))
            .block_task()
            .await
            .map_err(|e| Error::AgentSpawn(format!("agent initialize failed: {e}")))
    }

    /// Send session/new to an initialized agent.
    pub async fn new_session_on_agent(
        agent_cx: &ConnectionTo<agent_client_protocol::Agent>,
        cwd: &std::path::Path,
        mcp_servers: Vec<McpServer>,
    ) -> Result<NewSessionResponse, Error> {
        agent_cx
            .send_request(NewSessionRequest::new(cwd).mcp_servers(mcp_servers))
            .block_task()
            .await
            .map_err(|e| Error::AgentSpawn(format!("agent session/new failed: {e}")))
    }

    /// Send session/load to an initialized agent.
    pub async fn load_session_on_agent(
        agent_cx: &ConnectionTo<agent_client_protocol::Agent>,
        session_id: &str,
        cwd: &std::path::Path,
        mcp_servers: Vec<McpServer>,
    ) -> Result<(), Error> {
        agent_cx
            .send_request(
                LoadSessionRequest::new(SessionId::new(session_id), cwd).mcp_servers(mcp_servers),
            )
            .block_task()
            .await
            .map_err(|e| Error::AgentSpawn(format!("agent session/load failed: {e}")))?;
        Ok(())
    }

    pub async fn kill_agent(pid: u32) {
        use nix::sys::signal::{Signal, kill};
        use nix::unistd::Pid;

        let pid = Pid::from_raw(pid as i32);
        let _ = kill(pid, Signal::SIGTERM);
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let _ = kill(pid, Signal::SIGKILL);
    }
}
