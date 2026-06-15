mod transport;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::schema::{McpServer, SessionId};
use agent_client_protocol::{Client, DynConnectTo};
use jamsession::agent::AgentFactory;
use jamsession::error::Error;
use rhaicp::RhaiAgent;
use tokio::sync::mpsc;
use transport::UnixSocketTransport;

pub use expect_test;
pub use jamsession::LifecycleEvent;
pub use rhaicp::PriorSession;

/// Test implementation of `AgentFactory` that creates in-process RhaiAgent instances.
pub struct RhaiAgentFactory {
    new_session_script: Option<String>,
    prior_sessions: Vec<PriorSession>,
}

impl RhaiAgentFactory {
    pub fn new(config: &TestDaemonConfig) -> Self {
        Self {
            new_session_script: if config.agent_script.is_empty() {
                None
            } else {
                Some(config.agent_script.clone())
            },
            prior_sessions: config.prior_sessions.clone(),
        }
    }
}

impl AgentFactory for RhaiAgentFactory {
    fn create_transport(
        &self,
        session_id: &str,
        _cwd: &Path,
        _mcp_servers: &[McpServer],
    ) -> Result<DynConnectTo<Client>, Error> {
        let mut agent = RhaiAgent::new();
        if let Some(script) = &self.new_session_script {
            agent = agent.new_session_script(script.clone());
            if !session_id.is_empty() {
                let mut prior = self.prior_sessions.clone();
                prior.push(PriorSession {
                    session_id: SessionId::new(session_id),
                    script: script.clone(),
                });
                agent = agent.prior_sessions(prior);
            }
        }
        if !self.prior_sessions.is_empty() && self.new_session_script.is_none() {
            agent = agent.prior_sessions(self.prior_sessions.clone());
        }
        Ok(DynConnectTo::new(agent))
    }
}

/// Configuration for a test daemon instance.
pub struct TestDaemonConfig {
    pub idle_timeout: Duration,
    pub agent_script: String,
    pub prior_sessions: Vec<PriorSession>,
}

impl Default for TestDaemonConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(300),
            agent_script: String::new(),
            prior_sessions: Vec::new(),
        }
    }
}

/// An isolated daemon instance for integration testing.
pub struct TestDaemon {
    _temp_dir: tempfile::TempDir,
    socket_path: PathBuf,
    _daemon_handle: tokio::task::JoinHandle<()>,
    _drain_handle: tokio::task::JoinHandle<()>,
    events: Arc<Mutex<Vec<LifecycleEvent>>>,
    notify: Arc<tokio::sync::Notify>,
}

impl TestDaemon {
    /// Start a test daemon with the given configuration.
    /// Panics if the daemon doesn't become ready within 2 seconds.
    pub async fn start(config: TestDaemonConfig) -> Self {
        let temp_dir = tempfile::TempDir::new().expect("failed to create temp dir");
        let socket_path = temp_dir.path().join("daemon.sock");
        let state_path = temp_dir.path().join("state.json");

        let factory: Arc<dyn AgentFactory> = Arc::new(RhaiAgentFactory::new(&config));
        let idle_timeout = config.idle_timeout;

        let (lifecycle_tx, lifecycle_rx) = mpsc::unbounded_channel();

        let socket_path_clone = socket_path.clone();
        let daemon_handle = tokio::spawn(async move {
            let daemon =
                jamsession::daemon::Daemon::new_with_paths(&state_path, &socket_path_clone)
                    .with_factory(factory)
                    .with_idle_timeout(idle_timeout)
                    .with_quiescence_timeout(Duration::from_millis(10))
                    .with_send_guidelines(false)
                    .with_lifecycle_events(lifecycle_tx);
            let _ = daemon.run_v2().await;
        });

        let events: Arc<Mutex<Vec<LifecycleEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let notify = Arc::new(tokio::sync::Notify::new());

        let drain_handle = {
            let events = events.clone();
            let notify = notify.clone();
            tokio::spawn(async move {
                Self::drain_events(lifecycle_rx, events, notify).await;
            })
        };

        let this = Self {
            _temp_dir: temp_dir,
            socket_path,
            _daemon_handle: daemon_handle,
            _drain_handle: drain_handle,
            events,
            notify,
        };

        this.wait_for(
            |e| matches!(e, LifecycleEvent::Initialized),
            Duration::from_secs(2),
        )
        .await;

        this
    }

    async fn drain_events(
        mut rx: mpsc::UnboundedReceiver<LifecycleEvent>,
        events: Arc<Mutex<Vec<LifecycleEvent>>>,
        notify: Arc<tokio::sync::Notify>,
    ) {
        while let Some(event) = rx.recv().await {
            events.lock().unwrap().push(event);
            notify.notify_waiters();
        }
    }

    /// Block until a lifecycle event matching `predicate` is received, or timeout.
    pub async fn wait_for(
        &self,
        predicate: impl Fn(&LifecycleEvent) -> bool,
        timeout: Duration,
    ) -> LifecycleEvent {
        let result = tokio::time::timeout(timeout, async {
            let mut seen = 0;
            loop {
                let notified = self.notify.notified();
                {
                    let events = self.events.lock().unwrap();
                    while seen < events.len() {
                        if predicate(&events[seen]) {
                            return events[seen].clone();
                        }
                        seen += 1;
                    }
                }
                notified.await;
            }
        })
        .await;
        result.expect("timed out waiting for lifecycle event")
    }

    /// Assert the full lifecycle event trace matches the expected snapshot.
    /// Events with session IDs are normalized to `$session0`, `$session1`, etc.
    pub fn assert_lifecycle_events(&self, expected: expect_test::Expect) {
        let events = self.events.lock().unwrap();
        let mut session_ids: Vec<String> = Vec::new();
        let output: Vec<String> = events
            .iter()
            .map(|e| Self::normalize_event_display(e, &mut session_ids))
            .collect();
        expected.assert_eq(&output.join("\n"));
    }

    fn normalize_event_display(event: &LifecycleEvent, session_ids: &mut Vec<String>) -> String {
        let normalize_sid = |sid: &str, ids: &mut Vec<String>| -> String {
            if let Some(pos) = ids.iter().position(|s| s == sid) {
                format!("$session{pos}")
            } else {
                let pos = ids.len();
                ids.push(sid.to_string());
                format!("$session{pos}")
            }
        };

        match event {
            LifecycleEvent::Initialized => "Initialized".to_string(),
            LifecycleEvent::ClientConnected => "ClientConnected".to_string(),
            LifecycleEvent::ClientDisconnected { session_id: None } => {
                "ClientDisconnected".to_string()
            }
            LifecycleEvent::ClientDisconnected {
                session_id: Some(sid),
            } => {
                let normalized = normalize_sid(sid, session_ids);
                format!("ClientDisconnected({normalized})")
            }
            LifecycleEvent::SessionCreated { session_id } => {
                let normalized = normalize_sid(session_id, session_ids);
                format!("SessionCreated({normalized})")
            }
            LifecycleEvent::SessionLoaded { session_id } => {
                let normalized = normalize_sid(session_id, session_ids);
                format!("SessionLoaded({normalized})")
            }
            LifecycleEvent::SessionResumed { session_id } => {
                let normalized = normalize_sid(session_id, session_ids);
                format!("SessionResumed({normalized})")
            }
            LifecycleEvent::AgentQuiescent { session_id } => {
                let normalized = normalize_sid(session_id, session_ids);
                format!("AgentQuiescent({normalized})")
            }
            LifecycleEvent::AgentKilledIdle { session_id } => {
                let normalized = normalize_sid(session_id, session_ids);
                format!("AgentKilledIdle({normalized})")
            }
        }
    }

    /// Execute a Rhai client script against this daemon.
    /// Returns the script's last expression as a string.
    pub async fn execute_client(&self, script: &str) -> String {
        self.execute_client_with_cwd(script, Path::new("/tmp"))
            .await
    }

    /// Execute a Rhai client script with a specific cwd.
    pub async fn execute_client_with_cwd(&self, script: &str, cwd: &Path) -> String {
        let transport = UnixSocketTransport::new(&self.socket_path);
        rhaicp::client::RhaiClient::new()
            .cwd(cwd)
            .execute(transport, script)
            .await
            .expect("client script failed")
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

impl Drop for TestDaemon {
    fn drop(&mut self) {
        self._daemon_handle.abort();
        self._drain_handle.abort();
    }
}
