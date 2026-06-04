use std::path::PathBuf;
use std::sync::Arc;

use agent_client_protocol::{
    Agent, ByteStreams, ConnectionTo, Responder, on_receive_request,
    schema::{
        InitializeRequest, InitializeResponse, ListSessionsRequest, ListSessionsResponse,
        LoadSessionRequest, LoadSessionResponse, NewSessionRequest, NewSessionResponse,
        ResumeSessionRequest, ResumeSessionResponse, SessionInfo,
    },
};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::agent::AgentManager;
use crate::error::Error;
use crate::session::SessionManager;
use crate::state::DaemonState;

pub struct Daemon {
    state: Arc<Mutex<DaemonState>>,
    sessions: Arc<SessionManager>,
    socket_path: PathBuf,
    state_path: PathBuf,
}

impl Daemon {
    pub fn new(state_path: &std::path::Path) -> Self {
        let state = DaemonState::load(state_path);
        Self {
            state: Arc::new(Mutex::new(state)),
            sessions: Arc::new(SessionManager::new()),
            socket_path: Self::socket_path(),
            state_path: state_path.to_path_buf(),
        }
    }

    pub fn new_with_paths(state_path: &std::path::Path, socket_path: &std::path::Path) -> Self {
        let state = DaemonState::load(state_path);
        Self {
            state: Arc::new(Mutex::new(state)),
            sessions: Arc::new(SessionManager::new()),
            socket_path: socket_path.to_path_buf(),
            state_path: state_path.to_path_buf(),
        }
    }

    pub fn socket_path() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".academy")
            .join("daemon.sock")
    }

    pub async fn run(&self) -> Result<(), Error> {
        if let Some(parent) = self.socket_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Remove stale socket file
        let _ = tokio::fs::remove_file(&self.socket_path).await;

        let listener = UnixListener::bind(&self.socket_path)?;
        tracing::info!(path = %self.socket_path.display(), "daemon listening");

        loop {
            let (stream, _) = listener.accept().await?;
            let state = self.state.clone();
            let sessions = self.sessions.clone();
            let state_path = self.state_path.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_client(stream, state, sessions, state_path).await {
                    tracing::error!("client connection error: {e}");
                }
            });
        }
    }

    pub async fn shutdown(&self) {
        self.sessions.kill_all_agents().await;
        let _ = tokio::fs::remove_file(&self.socket_path).await;
        tracing::info!("daemon shut down");
    }
}

async fn handle_client(
    stream: UnixStream,
    state: Arc<Mutex<DaemonState>>,
    sessions: Arc<SessionManager>,
    state_path: PathBuf,
) -> Result<(), agent_client_protocol::Error> {
    let (read_half, write_half) = stream.into_split();
    let transport = ByteStreams::new(write_half.compat_write(), read_half.compat());

    let state_for_init = state.clone();
    let state_for_list = state.clone();
    let state_for_new = state.clone();
    let sessions_for_new = sessions.clone();
    let state_path_for_new = state_path.clone();
    let state_for_load = state.clone();
    let sessions_for_load = sessions.clone();
    let state_path_for_load = state_path.clone();
    let state_for_resume = state.clone();
    let sessions_for_resume = sessions.clone();
    let _state_path_for_resume = state_path;

    Agent
        .builder()
        .name("academy-daemon")
        .on_receive_request(
            async move |req: InitializeRequest,
                        responder: Responder<InitializeResponse>,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                let state = state_for_init.clone();
                cx.spawn(async move {
                    let response = handle_initialize(req, &state)
                        .await
                        .map_err(|e| agent_client_protocol::Error::from(&e))?;
                    responder.respond(response)
                })?;
                Ok(())
            },
            on_receive_request!(),
        )
        .on_receive_request(
            async move |req: ListSessionsRequest,
                        responder: Responder<ListSessionsResponse>,
                        _cx: ConnectionTo<agent_client_protocol::Client>| {
                let state = state_for_list.lock().await;
                let sessions = state.list_sessions_by_cwd(req.cwd.as_deref());
                let session_infos: Vec<SessionInfo> = sessions
                    .into_iter()
                    .map(|s| {
                        SessionInfo::new(s.session_id.clone(), s.cwd.clone())
                            .updated_at(s.updated_at.to_rfc3339())
                    })
                    .collect();
                responder.respond(ListSessionsResponse::new(session_infos))
            },
            on_receive_request!(),
        )
        .on_receive_request(
            async move |req: NewSessionRequest,
                        responder: Responder<NewSessionResponse>,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                let state = state_for_new.clone();
                let sessions = sessions_for_new.clone();
                let state_path = state_path_for_new.clone();
                cx.spawn(async move {
                    let result = sessions.handle_new_session(req, &state, &state_path).await;
                    match result {
                        Ok(response) => responder.respond(response),
                        Err(e) => {
                            responder.respond_with_error(agent_client_protocol::Error::from(&e))
                        }
                    }
                })?;
                Ok(())
            },
            on_receive_request!(),
        )
        .on_receive_request(
            async move |req: LoadSessionRequest,
                        responder: Responder<LoadSessionResponse>,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                let state = state_for_load.clone();
                let sessions = sessions_for_load.clone();
                let state_path = state_path_for_load.clone();
                cx.spawn(async move {
                    let result = sessions.handle_load_session(req, &state, &state_path).await;
                    match result {
                        Ok(response) => responder.respond(response),
                        Err(e) => {
                            responder.respond_with_error(agent_client_protocol::Error::from(&e))
                        }
                    }
                })?;
                Ok(())
            },
            on_receive_request!(),
        )
        .on_receive_request(
            async move |req: ResumeSessionRequest,
                        responder: Responder<ResumeSessionResponse>,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                let state = state_for_resume.clone();
                let sessions = sessions_for_resume.clone();
                cx.spawn(async move {
                    let result = sessions.handle_resume_session(req, &state).await;
                    match result {
                        Ok(response) => responder.respond(response),
                        Err(e) => {
                            responder.respond_with_error(agent_client_protocol::Error::from(&e))
                        }
                    }
                })?;
                Ok(())
            },
            on_receive_request!(),
        )
        .connect_to(transport)
        .await
}

async fn handle_initialize(
    req: InitializeRequest,
    state: &Mutex<DaemonState>,
) -> Result<InitializeResponse, Error> {
    let caps_value =
        serde_json::to_value(&req.client_capabilities).unwrap_or(serde_json::Value::Null);

    let state_guard = state.lock().await;
    if let Some(cached) = &state_guard.capabilities_cache
        && cached.matches(&caps_value)
    {
        let response: InitializeResponse = serde_json::from_value(cached.response.clone())
            .map_err(|e| Error::AgentSpawn(format!("corrupt capabilities cache: {e}")))?;
        return Ok(response);
    }
    drop(state_guard);

    // Cache miss: spawn a temp agent to get capabilities
    let response = AgentManager::get_capabilities(&req).await?;

    let response_value = serde_json::to_value(&response).unwrap_or(serde_json::Value::Null);
    let mut state_guard = state.lock().await;
    state_guard.capabilities_cache = Some(crate::state::CachedCapabilities {
        client_capabilities_hash: crate::state::CachedCapabilities::hash_capabilities(&caps_value),
        response: response_value,
    });
    let _ = state_guard.save(&DaemonState::state_path());
    drop(state_guard);

    Ok(response)
}
