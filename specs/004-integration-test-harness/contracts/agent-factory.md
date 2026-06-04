# Contract: AgentFactory Trait

The daemon's agent spawning is abstracted behind this trait, enabling test injection.

## Trait Definition

```rust
use std::path::Path;
use agent_client_protocol::schema::McpServer;
use agent_client_protocol::{Client, ConnectTo};

/// Factory for creating agent connections. The daemon calls this when
/// a session needs an agent (session/new, session/load with dead agent).
#[trait_variant::make(Send)]
pub trait AgentFactory: Send + Sync + 'static {
    /// Spawn or create an agent for the given session.
    /// Returns a transport that implements ConnectTo<Client> — the daemon
    /// connects to it as a Client to drive the agent.
    async fn spawn_agent(
        &self,
        session_id: &str,
        cwd: &Path,
        mcp_servers: Vec<McpServer>,
    ) -> Result<Box<dyn ConnectTo<Client>>, crate::error::Error>;
}
```

## Production Implementation

```rust
/// Production factory: spawns agent via acpr registry.
pub struct AcprFactory;

impl AgentFactory for AcprFactory {
    async fn spawn_agent(
        &self,
        _session_id: &str,
        _cwd: &Path,
        _mcp_servers: Vec<McpServer>,
    ) -> Result<Box<dyn ConnectTo<Client>>, Error> {
        Ok(Box::new(acpr::Acpr::new("claude-acp")))
    }
}
```

## Test Implementation

```rust
/// Test factory: creates RhaiAgent instances in-process.
pub struct RhaiAgentFactory {
    pub new_session_script: Option<String>,
    pub prior_sessions: Vec<PriorSession>,
}

impl AgentFactory for RhaiAgentFactory {
    async fn spawn_agent(
        &self,
        _session_id: &str,
        _cwd: &Path,
        _mcp_servers: Vec<McpServer>,
    ) -> Result<Box<dyn ConnectTo<Client>>, Error> {
        let mut agent = RhaiAgent::new().exit_panics(true);
        if let Some(script) = &self.new_session_script {
            agent = agent.new_session_script(script.clone());
        }
        if !self.prior_sessions.is_empty() {
            agent = agent.prior_sessions(self.prior_sessions.clone());
        }
        Ok(Box::new(agent))
    }
}
```

## Behavioral Contract

1. `spawn_agent` is called once per session activation (new or respawn after death)
2. The returned transport MUST implement `ConnectTo<Client>` and handle the full ACP lifecycle (initialize, session/new or session/load, prompt/start)
3. If the factory returns an error, the daemon MUST propagate it as an ACP error response to the requesting client
4. The factory MUST be called with the session ID and cwd from the client's request
5. The factory is shared across all sessions (`Arc<dyn AgentFactory>`) — it MUST be safe to call concurrently
