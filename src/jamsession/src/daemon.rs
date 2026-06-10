use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use agent_client_protocol::{
    Agent, ByteStreams, ConnectionTo, Responder, on_receive_request,
    schema::{
        InitializeRequest, InitializeResponse, ListSessionsRequest, ListSessionsResponse,
        LoadSessionRequest, LoadSessionResponse, NewSessionRequest, NewSessionResponse,
        ResumeSessionRequest, ResumeSessionResponse, SessionInfo,
    },
};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Notify;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::agent::{AgentManager, AgentTransport};
use crate::error::Error;
use crate::session::SessionManager;
use crate::state::DaemonState;

/// Long-running background process that listens on a Unix socket,
/// manages agent sessions, and bridges ACP traffic between clients and agents.
pub struct Daemon {
    /// Persistent session registry and capabilities cache, shared across client tasks.
    state: Arc<Mutex<DaemonState>>,
    /// In-memory session state (lifecycle, buffers, agent connections).
    sessions: Arc<SessionManager>,
    /// Path to the Unix domain socket (e.g. `~/.jamsession/daemon.sock`).
    socket_path: PathBuf,
    /// Path to the persistent state file (e.g. `~/.jamsession/state.json`).
    state_path: PathBuf,
    /// How to spawn agent processes (registry lookup or direct binary path).
    agent_transport: AgentTransport,
}

impl Daemon {
    pub fn new(state_path: &std::path::Path) -> Self {
        let state = DaemonState::load(state_path);
        Self {
            state: Arc::new(Mutex::new(state)),
            sessions: Arc::new(SessionManager::new()),
            socket_path: Self::socket_path(),
            state_path: state_path.to_path_buf(),
            agent_transport: AgentTransport::default(),
        }
    }

    pub fn new_with_paths(state_path: &std::path::Path, socket_path: &std::path::Path) -> Self {
        let state = DaemonState::load(state_path);
        Self {
            state: Arc::new(Mutex::new(state)),
            sessions: Arc::new(SessionManager::new()),
            socket_path: socket_path.to_path_buf(),
            state_path: state_path.to_path_buf(),
            agent_transport: AgentTransport::default(),
        }
    }

    pub fn with_agent_transport(mut self, transport: AgentTransport) -> Self {
        self.agent_transport = transport.clone();
        self.sessions = Arc::new(SessionManager::new().with_agent_transport(transport));
        self
    }

    pub fn with_idle_timeout(mut self, timeout: std::time::Duration) -> Self {
        let transport = self.agent_transport.clone();
        self.sessions = Arc::new(
            SessionManager::new()
                .with_agent_transport(transport)
                .with_idle_timeout(timeout),
        );
        self
    }

    pub fn socket_path() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".jamsession")
            .join("daemon.sock")
    }

    pub async fn run(&self) -> Result<(), Error> {
        // Rehydrate live sessions from persistent state
        {
            let state = self.state.lock().unwrap().clone();
            self.sessions.rehydrate_from_state(&state);
        }

        if let Some(parent) = self.socket_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let _ = tokio::fs::remove_file(&self.socket_path).await;

        let listener = UnixListener::bind(&self.socket_path)?;

        // FR-002: restrict socket to owner only
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o600);
            std::fs::set_permissions(&self.socket_path, perms)?;
        }

        tracing::info!(path = %self.socket_path.display(), "daemon listening");

        // ANCHOR: cwd-health-check
        // FR-005: periodic cwd health check
        {
            let sessions = self.sessions.clone();
            let state = self.state.clone();
            let state_path = self.state_path.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                    sessions.check_cwd_health(&state, &state_path);
                }
            });
        }
        // ANCHOR_END: cwd-health-check

        // ANCHOR: accept-loop
        loop {
            let (stream, _) = listener.accept().await?;
            let state = self.state.clone();
            let sessions = self.sessions.clone();
            let state_path = self.state_path.clone();
            let agent_transport = self.agent_transport.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    handle_client(stream, state, sessions, state_path, agent_transport).await
                {
                    tracing::error!("client connection error: {e}");
                }
            });
        }
        // ANCHOR_END: accept-loop
    }

    pub async fn shutdown(&self) {
        self.sessions.kill_all_agents();
        let _ = tokio::fs::remove_file(&self.socket_path).await;
        tracing::info!("daemon shut down");
    }
}

async fn handle_client(
    stream: UnixStream,
    state: Arc<Mutex<DaemonState>>,
    sessions: Arc<SessionManager>,
    state_path: PathBuf,
    agent_transport: AgentTransport,
) -> Result<(), agent_client_protocol::Error> {
    let (read_half, write_half) = stream.into_split();
    let transport = ByteStreams::new(write_half.compat_write(), read_half.compat());

    // T039: Cancel signal for this client connection — notified when a new client takes over
    let client_cancel: Arc<Notify> = Arc::new(Notify::new());

    // Track which session this client is associated with (set on session/new, load, or resume)
    let active_session_id: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    Agent
        .builder()
        .name("jamsession-daemon")
        .on_receive_request(
            {
                let state = state.clone();
                let agent_transport = agent_transport.clone();
                async move |req: InitializeRequest,
                            responder: Responder<InitializeResponse>,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let state = state.clone();
                    let agent_transport = agent_transport.clone();
                    cx.spawn(async move {
                        let response = handle_initialize(req, &state, &agent_transport)
                            .await
                            .map_err(|e| agent_client_protocol::Error::from(&e))?;
                        responder.respond(response)
                    })?;
                    Ok(())
                }
            },
            on_receive_request!(),
        )
        // ANCHOR: handle-session-list
        .on_receive_request(
            {
                let state = state.clone();
                async move |req: ListSessionsRequest,
                            responder: Responder<ListSessionsResponse>,
                            _cx: ConnectionTo<agent_client_protocol::Client>| {
                    let state = state.lock().unwrap();
                    let sessions = state.list_sessions_by_cwd(req.cwd.as_deref());
                    let session_infos: Vec<SessionInfo> = sessions
                        .into_iter()
                        .map(|s| {
                            SessionInfo::new(s.session_id.clone(), s.cwd.clone())
                                .updated_at(s.updated_at.to_rfc3339())
                        })
                        .collect();
                    responder.respond(ListSessionsResponse::new(session_infos))
                }
            },
            on_receive_request!(),
        )
        // ANCHOR_END: handle-session-list
        // ANCHOR: dispatch-session-new
        .on_receive_request(
            {
                let state = state.clone();
                let sessions = sessions.clone();
                let state_path = state_path.clone();
                let active_session_id = active_session_id.clone();
                let client_cancel = client_cancel.clone();
                async move |req: NewSessionRequest,
                            responder: Responder<NewSessionResponse>,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let state = state.clone();
                    let sessions = sessions.clone();
                    let state_path = state_path.clone();
                    let active_session_id = active_session_id.clone();
                    let client_cancel = client_cancel.clone();
                    let cx2 = cx.clone();
                    cx.spawn(async move {
                        let result = sessions
                            .handle_new_session(req, &state, &state_path, &cx2)
                            .await;
                        match result {
                            Ok(response) => {
                                let sid = response.session_id.0.to_string();
                                *active_session_id.lock().unwrap() = Some(sid.clone());
                                sessions.register_client_cancel(&sid, client_cancel);
                                responder.respond(response)
                            }
                            Err(e) => {
                                responder.respond_with_error(agent_client_protocol::Error::from(&e))
                            }
                        }
                    })?;
                    Ok(())
                }
            },
            on_receive_request!(),
        )
        // ANCHOR_END: dispatch-session-new
        // ANCHOR: dispatch-session-load
        .on_receive_request(
            {
                let state = state.clone();
                let sessions = sessions.clone();
                let state_path = state_path.clone();
                let active_session_id = active_session_id.clone();
                let client_cancel = client_cancel.clone();
                async move |req: LoadSessionRequest,
                            responder: Responder<LoadSessionResponse>,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let state = state.clone();
                    let sessions = sessions.clone();
                    let state_path = state_path.clone();
                    let active_session_id = active_session_id.clone();
                    let client_cancel = client_cancel.clone();
                    let sid = req.session_id.0.to_string();
                    let cx2 = cx.clone();
                    cx.spawn(async move {
                        let result = sessions
                            .handle_load_session(req, &state, &state_path, &cx2)
                            .await;
                        match result {
                            Ok(response) => {
                                *active_session_id.lock().unwrap() = Some(sid.clone());
                                sessions.register_client_cancel(&sid, client_cancel);
                                responder.respond(response)
                            }
                            Err(e) => {
                                responder.respond_with_error(agent_client_protocol::Error::from(&e))
                            }
                        }
                    })?;
                    Ok(())
                }
            },
            on_receive_request!(),
        )
        // ANCHOR_END: dispatch-session-load
        .on_receive_request(
            {
                let state = state.clone();
                let sessions = sessions.clone();
                let active_session_id = active_session_id.clone();
                let client_cancel = client_cancel.clone();
                async move |req: ResumeSessionRequest,
                            responder: Responder<ResumeSessionResponse>,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let state = state.clone();
                    let sessions = sessions.clone();
                    let active_session_id = active_session_id.clone();
                    let client_cancel = client_cancel.clone();
                    let sid = req.session_id.0.to_string();
                    let cx2 = cx.clone();
                    cx.spawn(async move {
                        let result = sessions.handle_resume_session(req, &state, &cx2).await;
                        match result {
                            Ok(response) => {
                                *active_session_id.lock().unwrap() = Some(sid.clone());
                                sessions.register_client_cancel(&sid, client_cancel);
                                responder.respond(response)
                            }
                            Err(e) => {
                                responder.respond_with_error(agent_client_protocol::Error::from(&e))
                            }
                        }
                    })?;
                    Ok(())
                }
            },
            on_receive_request!(),
        )
        .connect_to(transport)
        .await?;

    // ANCHOR: client-disconnect
    // T037: Connection closed — trigger disconnect_client
    let session_id = active_session_id.lock().unwrap().clone();
    if let Some(sid) = session_id {
        tracing::debug!(session_id = sid, "client disconnected");
        sessions.disconnect_client(&sid);
    }
    // ANCHOR_END: client-disconnect

    Ok(())
}

// ANCHOR: handle-initialize
async fn handle_initialize(
    req: InitializeRequest,
    state: &Mutex<DaemonState>,
    agent_transport: &AgentTransport,
) -> Result<InitializeResponse, Error> {
    let caps_value =
        serde_json::to_value(&req.client_capabilities).unwrap_or(serde_json::Value::Null);

    {
        let state_guard = state.lock().unwrap();
        if let Some(cached) = &state_guard.capabilities_cache
            && cached.matches(&caps_value)
        {
            let response: InitializeResponse = serde_json::from_value(cached.response.clone())
                .map_err(|e| Error::AgentSpawn(format!("corrupt capabilities cache: {e}")))?;
            return Ok(response);
        }
    }

    let response = AgentManager::get_capabilities(&req, agent_transport).await?;

    let response_value = serde_json::to_value(&response).unwrap_or(serde_json::Value::Null);
    {
        let mut state_guard = state.lock().unwrap();
        state_guard.capabilities_cache = Some(crate::state::CachedCapabilities {
            client_capabilities_hash: crate::state::CachedCapabilities::hash_capabilities(
                &caps_value,
            ),
            response: response_value,
        });
        let _ = state_guard.save(&DaemonState::state_path());
    }

    Ok(response)
}
// ANCHOR_END: handle-initialize
