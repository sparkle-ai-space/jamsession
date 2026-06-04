use agent_client_protocol::schema::{InitializeRequest, InitializeResponse, ProtocolVersion};
use agent_client_protocol::{Client, ConnectionTo};

use crate::error::Error;

pub struct AgentManager;

impl AgentManager {
    pub async fn get_capabilities(req: &InitializeRequest) -> Result<InitializeResponse, Error> {
        let agent_transport = acpr::Acpr::new("claude-acp");
        let init_req = req.clone();

        let response = Client
            .builder()
            .name("academy-daemon-caps")
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

    pub async fn spawn_for_session(
        session_id: &str,
        cwd: &std::path::Path,
        mcp_servers: Vec<agent_client_protocol::schema::McpServer>,
    ) -> Result<ConnectionTo<agent_client_protocol::Agent>, Error> {
        let _ = (session_id, cwd, mcp_servers);
        // TODO: Phase 3 implementation - spawn agent and connect
        Err(Error::AgentSpawn("not yet implemented".to_string()))
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
