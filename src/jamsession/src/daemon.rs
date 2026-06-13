use std::path::PathBuf;
use std::sync::Arc;

use agent_client_protocol::{
    Agent, ByteStreams, ConnectionTo, Dispatch, HandleDispatchFrom, Handled, Responder,
    on_receive_request,
    schema::{
        InitializeRequest, InitializeResponse, ListSessionsRequest, ListSessionsResponse,
        LoadSessionRequest, LoadSessionResponse, NewSessionRequest, NewSessionResponse,
        ResumeSessionRequest, ResumeSessionResponse, SessionNotification,
    },
};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::actor::{DaemonActor, DaemonMessage, install_agent_forwarder, install_client_forwarder};
use crate::agent::{AgentFactory, AgentManager};
use crate::error::Error;
use crate::session::{LifecycleEvent, LifecycleEventSender};
use crate::state::DaemonState;

pub struct Daemon {
    socket_path: PathBuf,
    state_path: PathBuf,
    factory: Arc<dyn AgentFactory>,
    idle_timeout: std::time::Duration,
    quiescence_timeout: std::time::Duration,
    send_guidelines: bool,
    lifecycle_tx: Option<LifecycleEventSender>,
}

impl Daemon {
    pub fn new(state_path: &std::path::Path) -> Self {
        Self {
            socket_path: Self::socket_path(),
            state_path: state_path.to_path_buf(),
            factory: Arc::new(crate::agent::AcprFactory::default()),
            idle_timeout: std::time::Duration::from_secs(900),
            quiescence_timeout: std::time::Duration::from_secs(10),
            send_guidelines: true,
            lifecycle_tx: None,
        }
    }

    pub fn new_with_paths(state_path: &std::path::Path, socket_path: &std::path::Path) -> Self {
        Self {
            socket_path: socket_path.to_path_buf(),
            state_path: state_path.to_path_buf(),
            factory: Arc::new(crate::agent::AcprFactory::default()),
            idle_timeout: std::time::Duration::from_secs(900),
            quiescence_timeout: std::time::Duration::from_secs(10),
            send_guidelines: true,
            lifecycle_tx: None,
        }
    }

    pub fn with_factory(mut self, factory: Arc<dyn AgentFactory>) -> Self {
        self.factory = factory;
        self
    }

    pub fn with_idle_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    pub fn with_quiescence_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.quiescence_timeout = timeout;
        self
    }

    pub fn with_send_guidelines(mut self, send: bool) -> Self {
        self.send_guidelines = send;
        self
    }

    pub fn with_lifecycle_events(mut self, tx: LifecycleEventSender) -> Self {
        self.lifecycle_tx = Some(tx);
        self
    }

    pub fn socket_path() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".jamsession")
            .join("daemon.sock")
    }

    pub async fn run(&self) -> Result<(), Error> {
        let state = DaemonState::load(&self.state_path);

        let (actor_tx, actor_rx) = mpsc::unbounded_channel();

        let mut actor = DaemonActor::new(
            state,
            self.state_path.clone(),
            self.factory.clone(),
            self.idle_timeout,
            self.quiescence_timeout,
            self.lifecycle_tx.clone(),
            actor_tx.clone(),
        );

        // ANCHOR: cwd-health-check-timer
        {
            let tx = actor_tx.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                    let _ = tx.send(DaemonMessage::CwdHealthCheck);
                }
            });
        }
        // ANCHOR_END: cwd-health-check-timer

        // Spawn actor task
        tokio::spawn(async move {
            actor.run(actor_rx).await;
        });

        if let Some(parent) = self.socket_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let _ = tokio::fs::remove_file(&self.socket_path).await;

        let listener = UnixListener::bind(&self.socket_path)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o600);
            std::fs::set_permissions(&self.socket_path, perms)?;
        }

        tracing::info!(path = %self.socket_path.display(), "daemon listening");

        if let Some(tx) = &self.lifecycle_tx {
            let _ = tx.send(LifecycleEvent::Initialized);
        }

        // ANCHOR: accept-loop
        loop {
            let (stream, _) = listener.accept().await?;
            let tx = actor_tx.clone();
            let factory = self.factory.clone();
            let send_guidelines = self.send_guidelines;
            let lifecycle_tx = self.lifecycle_tx.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    handle_client(stream, tx, factory, send_guidelines, lifecycle_tx).await
                {
                    tracing::error!("client connection error: {e}");
                }
            });
        }
        // ANCHOR_END: accept-loop
    }

    pub async fn shutdown(&self) {
        let _ = tokio::fs::remove_file(&self.socket_path).await;
        tracing::info!("daemon shut down");
    }
}

// ANCHOR: handle-client
async fn handle_client(
    stream: UnixStream,
    actor_tx: mpsc::UnboundedSender<DaemonMessage>,
    factory: Arc<dyn AgentFactory>,
    send_guidelines: bool,
    lifecycle_tx: Option<LifecycleEventSender>,
) -> Result<(), agent_client_protocol::Error> {
    if let Some(tx) = &lifecycle_tx {
        let _ = tx.send(LifecycleEvent::ClientConnected);
    }

    let (read_half, write_half) = stream.into_split();
    let transport = ByteStreams::new(write_half.compat_write(), read_half.compat());

    let active_sessions: Arc<std::sync::Mutex<Vec<String>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    Agent
        .builder()
        .name("jamsession-daemon")
        // ANCHOR: handle-initialize
        .on_receive_request(
            async |req: InitializeRequest,
                   responder: Responder<InitializeResponse>,
                   cx: ConnectionTo<agent_client_protocol::Client>| {
                let actor_tx = actor_tx.clone();
                cx.spawn(async move {
                    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                    let _ = actor_tx.send(DaemonMessage::Initialize {
                        req,
                        reply: reply_tx,
                    });
                    match reply_rx.await {
                        Ok(Ok(response)) => responder.respond(response),
                        Ok(Err(e)) => {
                            responder.respond_with_error(agent_client_protocol::Error::from(&e))
                        }
                        Err(_) => responder.respond_with_error(
                            agent_client_protocol::Error::internal_error()
                                .data("actor channel closed"),
                        ),
                    }
                })?;
                Ok(())
            },
            on_receive_request!(),
        )
        // ANCHOR_END: handle-initialize
        // ANCHOR: handle-session-list
        .on_receive_request(
            async |req: ListSessionsRequest,
                   responder: Responder<ListSessionsResponse>,
                   cx: ConnectionTo<agent_client_protocol::Client>| {
                let actor_tx = actor_tx.clone();
                cx.spawn(async move {
                    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                    let _ = actor_tx.send(DaemonMessage::ListSessions {
                        req,
                        reply: reply_tx,
                    });
                    match reply_rx.await {
                        Ok(response) => responder.respond(response),
                        Err(_) => responder.respond_with_error(
                            agent_client_protocol::Error::internal_error()
                                .data("actor channel closed"),
                        ),
                    }
                })?;
                Ok(())
            },
            on_receive_request!(),
        )
        // ANCHOR_END: handle-session-list
        // ANCHOR: dispatch-session-new
        .on_receive_request(
            async |req: NewSessionRequest,
                   responder: Responder<NewSessionResponse>,
                   cx: ConnectionTo<agent_client_protocol::Client>| {
                let actor_tx = actor_tx.clone();
                let factory = factory.clone();
                let active_sessions = active_sessions.clone();
                let cx2 = cx.clone();
                cx.spawn(async move {
                    let result = handle_session_new(
                        req,
                        &cx2,
                        &actor_tx,
                        factory.as_ref(),
                        send_guidelines,
                    )
                    .await;
                    match result {
                        Ok(response) => {
                            let sid = response.session_id.0.to_string();
                            active_sessions.lock().unwrap().push(sid);
                            responder.respond(response)
                        }
                        Err(e) => {
                            responder.respond_with_error(agent_client_protocol::Error::from(&e))
                        }
                    }
                })?;
                Ok(())
            },
            on_receive_request!(),
        )
        // ANCHOR_END: dispatch-session-new
        // ANCHOR: dispatch-session-load
        .on_receive_request(
            async |req: LoadSessionRequest,
                   responder: Responder<LoadSessionResponse>,
                   cx: ConnectionTo<agent_client_protocol::Client>| {
                let actor_tx = actor_tx.clone();
                let factory = factory.clone();
                let active_sessions = active_sessions.clone();
                let cx2 = cx.clone();
                cx.spawn(async move {
                    let result =
                        handle_session_load(req, &cx2, &actor_tx, factory.as_ref()).await;
                    match result {
                        Ok((response, sid)) => {
                            active_sessions.lock().unwrap().push(sid);
                            responder.respond(response)
                        }
                        Err(e) => {
                            responder.respond_with_error(agent_client_protocol::Error::from(&e))
                        }
                    }
                })?;
                Ok(())
            },
            on_receive_request!(),
        )
        // ANCHOR_END: dispatch-session-load
        .on_receive_request(
            async |req: ResumeSessionRequest,
                   responder: Responder<ResumeSessionResponse>,
                   cx: ConnectionTo<agent_client_protocol::Client>| {
                let actor_tx = actor_tx.clone();
                let factory = factory.clone();
                let active_sessions = active_sessions.clone();
                let cx2 = cx.clone();
                cx.spawn(async move {
                    let result =
                        handle_session_resume(req, &cx2, &actor_tx, factory.as_ref()).await;
                    match result {
                        Ok((response, sid)) => {
                            active_sessions.lock().unwrap().push(sid);
                            responder.respond(response)
                        }
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
        .await?;

    // ANCHOR_END: handle-client

    // ANCHOR: client-disconnect
    let sessions = active_sessions.lock().unwrap().clone();
    for sid in &sessions {
        tracing::debug!(session_id = sid.as_str(), "client disconnected");
        let _ = actor_tx.send(DaemonMessage::ClientDisconnected {
            session_id: sid.clone(),
        });
    }

    if let Some(tx) = &lifecycle_tx {
        let session_id = sessions.into_iter().last();
        let _ = tx.send(LifecycleEvent::ClientDisconnected { session_id });
    }
    // ANCHOR_END: client-disconnect

    Ok(())
}

// ---------------------------------------------------------------------------
// Session handlers — run inside cx.spawn(...) so block_task() is safe
// ---------------------------------------------------------------------------

// ANCHOR: handle-session-new
async fn handle_session_new(
    req: NewSessionRequest,
    client_cx: &ConnectionTo<agent_client_protocol::Client>,
    actor_tx: &mpsc::UnboundedSender<DaemonMessage>,
    factory: &dyn AgentFactory,
    send_guidelines: bool,
) -> Result<NewSessionResponse, Error> {
    if !req.cwd.is_absolute() || !req.cwd.exists() {
        return Err(Error::InvalidCwd { path: req.cwd });
    }

    let agent_cx =
        AgentManager::spawn_agent_connection(client_cx, factory, "", &req.cwd, &req.mcp_servers)?;
    AgentManager::initialize_agent(&agent_cx).await?;
    let agent_response =
        AgentManager::new_session_on_agent(&agent_cx, &req.cwd, req.mcp_servers).await?;
    let session_id = agent_response.session_id.0.to_string();

    if send_guidelines {
        use agent_client_protocol::schema::{ContentBlock, PromptRequest, SessionId, TextContent};
        static GUIDELINES: &str = include_str!("guidelines.md");
        let guidelines_prompt = PromptRequest::new(
            SessionId::new(session_id.as_str()),
            vec![ContentBlock::Text(TextContent::new(GUIDELINES))],
        );
        agent_cx
            .send_request(guidelines_prompt)
            .block_task()
            .await
            .map_err(|e| Error::AgentSpawn(format!("guidelines delivery failed: {e}")))?;
    }

    // Install forwarders BEFORE returning the response — the client may send
    // a prompt immediately after receiving the session/new response.
    install_agent_forwarder(&agent_cx, &session_id, actor_tx.clone())?;
    install_client_forwarder(client_cx, &session_id, actor_tx.clone())?;

    let _ = actor_tx.send(DaemonMessage::SessionCreated {
        session_id: session_id.clone(),
        cwd: req.cwd,
        client_cx: client_cx.clone(),
        agent_cx,
    });

    Ok(NewSessionResponse::new(session_id))
}
// ANCHOR_END: handle-session-new

// ANCHOR: handle-session-load
async fn handle_session_load(
    req: LoadSessionRequest,
    client_cx: &ConnectionTo<agent_client_protocol::Client>,
    actor_tx: &mpsc::UnboundedSender<DaemonMessage>,
    factory: &dyn AgentFactory,
) -> Result<(LoadSessionResponse, String), Error> {
    let session_id = req.session_id.0.to_string();

    // Ask the actor about session state
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let _ = actor_tx.send(DaemonMessage::QuerySessionState {
        session_id: session_id.clone(),
        reply: reply_tx,
    });
    let info = reply_rx
        .await
        .map_err(|_| Error::AgentSpawn("actor channel closed".into()))?
        .ok_or_else(|| Error::SessionNotFound(session_id.clone()))?;

    let new_agent_cx = if info.agent_dead {
        let agent_cx = AgentManager::spawn_agent_connection(
            client_cx,
            factory,
            &session_id,
            &info.cwd,
            &req.mcp_servers,
        )?;
        AgentManager::initialize_agent(&agent_cx).await?;

        let replay_buffer: Arc<std::sync::Mutex<Vec<serde_json::Value>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        agent_cx
            .add_dynamic_handler(ReplayCapture::new(replay_buffer.clone()))
            .map_err(|e| Error::AgentSpawn(format!("replay capture: {e}")))?
            .run_indefinitely();

        AgentManager::load_session_on_agent(&agent_cx, &session_id, &info.cwd, req.mcp_servers)
            .await?;

        {
            let buf = replay_buffer.lock().unwrap();
            for msg in buf.iter() {
                if let Ok(notif) = serde_json::from_value::<SessionNotification>(msg.clone()) {
                    let _ = client_cx.send_notification(notif);
                }
            }
        }

        install_agent_forwarder(&agent_cx, &session_id, actor_tx.clone())?;
        Some(agent_cx)
    } else {
        None
    };

    // Install client forwarder BEFORE returning the response
    install_client_forwarder(client_cx, &session_id, actor_tx.clone())?;

    let _ = actor_tx.send(DaemonMessage::SessionReconnected {
        session_id: session_id.clone(),
        client_cx: client_cx.clone(),
        agent_cx: new_agent_cx,
        replay_to_client: !info.agent_dead,
    });

    Ok((LoadSessionResponse::new(), session_id))
}
// ANCHOR_END: handle-session-load

async fn handle_session_resume(
    req: ResumeSessionRequest,
    client_cx: &ConnectionTo<agent_client_protocol::Client>,
    actor_tx: &mpsc::UnboundedSender<DaemonMessage>,
    factory: &dyn AgentFactory,
) -> Result<(ResumeSessionResponse, String), Error> {
    let session_id = req.session_id.0.to_string();

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let _ = actor_tx.send(DaemonMessage::QuerySessionState {
        session_id: session_id.clone(),
        reply: reply_tx,
    });
    let info = reply_rx
        .await
        .map_err(|_| Error::AgentSpawn("actor channel closed".into()))?
        .ok_or_else(|| Error::SessionNotFound(session_id.clone()))?;

    let new_agent_cx = if info.agent_dead {
        let agent_cx = AgentManager::spawn_agent_connection(
            client_cx,
            factory,
            &session_id,
            &info.cwd,
            &req.mcp_servers,
        )?;
        AgentManager::initialize_agent(&agent_cx).await?;
        AgentManager::load_session_on_agent(&agent_cx, &session_id, &info.cwd, req.mcp_servers)
            .await?;
        install_agent_forwarder(&agent_cx, &session_id, actor_tx.clone())?;
        Some(agent_cx)
    } else {
        None
    };

    install_client_forwarder(client_cx, &session_id, actor_tx.clone())?;

    let _ = actor_tx.send(DaemonMessage::SessionReconnected {
        session_id: session_id.clone(),
        client_cx: client_cx.clone(),
        agent_cx: new_agent_cx,
        replay_to_client: false,
    });

    Ok((ResumeSessionResponse::new(), session_id))
}

// ---------------------------------------------------------------------------
// ReplayCapture — captures notifications during session/load for replay
// ---------------------------------------------------------------------------

struct ReplayCapture {
    buffer: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
}

impl ReplayCapture {
    fn new(buffer: Arc<std::sync::Mutex<Vec<serde_json::Value>>>) -> Self {
        Self { buffer }
    }
}

impl HandleDispatchFrom<agent_client_protocol::Agent> for ReplayCapture {
    async fn handle_dispatch_from(
        &mut self,
        message: Dispatch,
        _agent_cx: ConnectionTo<agent_client_protocol::Agent>,
    ) -> agent_client_protocol::schema::Result<Handled<Dispatch>> {
        if let Dispatch::Notification(ref notif) = message
            && let Ok(value) = serde_json::to_value(notif)
        {
            self.buffer.lock().unwrap().push(value);
        }
        Ok(Handled::No {
            message,
            retry: false,
        })
    }

    fn describe_chain(&self) -> impl std::fmt::Debug {
        "ReplayCapture"
    }
}
