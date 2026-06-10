use std::path::PathBuf;
use std::str::FromStr;

use agent_client_protocol::schema::{
    InitializeRequest, InitializeResponse, LoadSessionRequest, McpServer, NewSessionRequest,
    NewSessionResponse, ProtocolVersion, SessionId,
};
use agent_client_protocol::{AcpAgent, Client, ConnectionTo, DynConnectTo};

use crate::error::Error;

/// Configuration for how the daemon spawns agent processes.
#[derive(Debug, Clone)]
pub enum AgentTransport {
    /// Use acpr registry to resolve agent by name (production).
    Registry(String),
    /// Use a specific binary path (testing).
    Binary(PathBuf),
}

impl Default for AgentTransport {
    fn default() -> Self {
        Self::Registry("claude-acp".to_string())
    }
}

impl AgentTransport {
    fn make_transport(&self) -> DynConnectTo<Client> {
        match self {
            Self::Registry(name) => DynConnectTo::new(acpr::Acpr::new(name)),
            Self::Binary(path) => {
                let cmd = path.display().to_string();
                DynConnectTo::new(AcpAgent::from_str(&cmd).expect("valid agent command"))
            }
        }
    }
}

pub struct AgentManager;

impl AgentManager {
    /// Probe agent capabilities by spawning a short-lived "temp agent" (FR-008).
    /// We spin up a full agent connection just to exchange `initialize`, then drop it.
    /// The response is cached daemon-side so subsequent clients with the same
    /// clientCapabilities skip this entirely.
    pub async fn get_capabilities(
        req: &InitializeRequest,
        transport_config: &AgentTransport,
    ) -> Result<InitializeResponse, Error> {
        let agent_transport = transport_config.make_transport();
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

    // ANCHOR: spawn-agent
    /// Spawn an agent subprocess and return the connection handle.
    pub fn spawn_agent_connection(
        client_cx: &ConnectionTo<agent_client_protocol::Client>,
        transport_config: &AgentTransport,
    ) -> Result<ConnectionTo<agent_client_protocol::Agent>, Error> {
        let agent_transport = transport_config.make_transport();
        client_cx
            .spawn_connection(
                Client.builder().name("jamsession-daemon-agent"),
                agent_transport,
            )
            .map_err(|e| Error::AgentSpawn(format!("failed to spawn agent connection: {e}")))
    }
    // ANCHOR_END: spawn-agent

    // ANCHOR: initialize-agent
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
    // ANCHOR_END: initialize-agent

    // ANCHOR: new-session-on-agent
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
    // ANCHOR_END: new-session-on-agent

    // ANCHOR: load-session-on-agent
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
    // ANCHOR_END: load-session-on-agent

    pub async fn kill_agent(pid: u32) {
        use nix::sys::signal::{Signal, kill};
        use nix::unistd::Pid;

        let pid = Pid::from_raw(pid as i32);
        let _ = kill(pid, Signal::SIGTERM);
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let _ = kill(pid, Signal::SIGKILL);
    }
}
