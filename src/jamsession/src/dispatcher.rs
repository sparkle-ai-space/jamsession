use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use agent_client_protocol::schema::{
    InitializeRequest, InitializeResponse, ListSessionsRequest, ListSessionsResponse,
    LoadSessionRequest, LoadSessionResponse, McpServer, NewSessionRequest, NewSessionResponse,
    ProtocolVersion, ResumeSessionRequest, ResumeSessionResponse, SessionConfigOptionCategory,
    SessionId as AcpSessionId, SessionInfo, SetSessionConfigOptionRequest,
};
use agent_client_protocol::util::MatchDispatch;
use agent_client_protocol::{
    Agent, ByteStreams, Client, Dispatch, DynConnectTo, HandleDispatchFrom, Handled,
    JsonRpcResponse, Responder,
};
use chrono::Utc;
use futures::StreamExt;
use scope_tasks::TaskSpawner;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::agent::AgentFactory;
use crate::db::{NewTrace, SessionRecord, Store, TraceDirection, TraceKind};
use crate::session::{LifecycleEvent, LifecycleEventSender};

// ---------------------------------------------------------------------------
// IDs
// ---------------------------------------------------------------------------

type ClientId = u64;
type AgentId = u64;
type SessionId = String;

// ---------------------------------------------------------------------------
// DispatcherMessage
// ---------------------------------------------------------------------------

// ANCHOR: daemon-message
pub(super) enum DispatcherMessage {
    // --- Pipe registration/teardown ---
    ClientRegistered {
        client_id: ClientId,
        outgoing_tx: mpsc::UnboundedSender<Dispatch>,
    },
    ClientDisconnected {
        client_id: ClientId,
    },
    AgentReady {
        agent_id: AgentId,
        outgoing_tx: mpsc::UnboundedSender<Dispatch>,
        session_id: SessionId,
        client_id: ClientId,
        cwd: PathBuf,
        responder: AgentReadyResponder,
    },
    AgentCapabilities {
        capabilities: Box<InitializeResponse>,
    },
    AgentDisconnected {
        agent_id: AgentId,
    },

    // --- Forwarded dispatches ---
    FromClient {
        client_id: ClientId,
        dispatch: Dispatch,
    },
    FromAgent {
        agent_id: AgentId,
        dispatch: Dispatch,
    },

    // --- Timers ---
    AgentQuiescent {
        session_id: SessionId,
        generation: u64,
    },
    IdleTimeoutElapsed {
        session_id: SessionId,
        generation: u64,
    },
    CwdHealthCheck,

    // --- Trace capture ---
    ResponseSent {
        client_id: ClientId,
        method: String,
        request_id: String,
        payload: serde_json::Value,
    },
    ModelSet {
        session_id: SessionId,
        from: String,
        to: String,
    },
}
// ANCHOR_END: daemon-message

// ---------------------------------------------------------------------------
// LifecycleState
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
enum LifecycleState {
    AgentDead,
    Active,
    TurnComplete,
    Quiescent,
    IdleTimerRunning,
}

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

struct Session {
    agent_id: AgentId,
    client_ids: Vec<ClientId>,
    generation: u64,
    lifecycle_state: LifecycleState,
    respawn_attempted: bool,
    record: SessionRecord,
}

// ---------------------------------------------------------------------------
// Handles
// ---------------------------------------------------------------------------

struct ClientHandle {
    outgoing_tx: mpsc::UnboundedSender<Dispatch>,
}

struct AgentHandle {
    outgoing_tx: mpsc::UnboundedSender<Dispatch>,
}

// ---------------------------------------------------------------------------
// AgentReadyResponder
// ---------------------------------------------------------------------------

#[expect(clippy::enum_variant_names)]
pub(super) enum AgentReadyResponder {
    NewSession(Responder<NewSessionResponse>),
    LoadSession(Responder<LoadSessionResponse>),
    ResumeSession(Responder<ResumeSessionResponse>),
}

fn respond_agent_ready_error(responder: AgentReadyResponder, err: agent_client_protocol::Error) {
    match responder {
        AgentReadyResponder::NewSession(r) => {
            let _ = r.respond_with_error(err);
        }
        AgentReadyResponder::LoadSession(r) => {
            let _ = r.respond_with_error(err);
        }
        AgentReadyResponder::ResumeSession(r) => {
            let _ = r.respond_with_error(err);
        }
    }
}

// ---------------------------------------------------------------------------
// Dispatcher
// ---------------------------------------------------------------------------

pub(super) struct Dispatcher<'scope> {
    tasks: TaskSpawner<'scope, crate::error::Error>,
    clients: HashMap<ClientId, ClientHandle>,
    agents: HashMap<AgentId, AgentHandle>,
    sessions: HashMap<SessionId, Session>,
    client_to_session: HashMap<ClientId, SessionId>,
    agent_to_session: HashMap<AgentId, SessionId>,
    store: Store,
    capabilities: Option<InitializeResponse>,
    factory: Arc<dyn AgentFactory>,
    idle_timeout: std::time::Duration,
    quiescence_timeout: std::time::Duration,
    send_guidelines: bool,
    default_model: Option<String>,
    event_tx: Option<LifecycleEventSender>,
    dispatcher_tx: mpsc::UnboundedSender<DispatcherMessage>,
    trace: bool,
    next_agent_id: u64,
}

impl<'scope> Dispatcher<'scope> {
    #[expect(clippy::too_many_arguments)]
    pub(super) async fn new(
        tasks: TaskSpawner<'scope, crate::error::Error>,
        store: Store,
        factory: Arc<dyn AgentFactory>,
        idle_timeout: std::time::Duration,
        quiescence_timeout: std::time::Duration,
        send_guidelines: bool,
        default_model: Option<String>,
        event_tx: Option<LifecycleEventSender>,
        dispatcher_tx: mpsc::UnboundedSender<DispatcherMessage>,
        trace: bool,
    ) -> crate::error::Result<Self> {
        let mut dispatcher = Self {
            tasks,
            clients: HashMap::new(),
            agents: HashMap::new(),
            sessions: HashMap::new(),
            client_to_session: HashMap::new(),
            agent_to_session: HashMap::new(),
            store,
            capabilities: None,
            factory,
            idle_timeout,
            quiescence_timeout,
            send_guidelines,
            default_model,
            event_tx,
            dispatcher_tx,
            trace,
            next_agent_id: 1,
        };
        dispatcher.rehydrate_from_store().await?;
        Ok(dispatcher)
    }

    async fn rehydrate_from_store(&mut self) -> crate::error::Result<()> {
        for record in self.store.list_sessions(None).await? {
            if !self.sessions.contains_key(&record.session_id) {
                self.sessions.insert(
                    record.session_id.clone(),
                    Session {
                        agent_id: 0,
                        client_ids: Vec::new(),
                        generation: 0,
                        lifecycle_state: LifecycleState::AgentDead,
                        respawn_attempted: false,
                        record,
                    },
                );
            }
        }
        Ok(())
    }

    pub(super) async fn run(&mut self, mut rx: mpsc::UnboundedReceiver<DispatcherMessage>) {
        while let Some(msg) = rx.recv().await {
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(&mut self, msg: DispatcherMessage) {
        match msg {
            DispatcherMessage::ClientRegistered {
                client_id,
                outgoing_tx,
            } => {
                self.clients.insert(client_id, ClientHandle { outgoing_tx });
                self.trace_event(
                    None,
                    "acp-client",
                    "client_connected",
                    serde_json::json!({}),
                )
                .await;
                self.emit(LifecycleEvent::ClientConnected);
            }
            DispatcherMessage::ClientDisconnected { client_id } => {
                self.handle_client_disconnected(client_id).await;
            }
            DispatcherMessage::AgentReady {
                agent_id,
                outgoing_tx,
                session_id,
                client_id,
                cwd,
                responder,
            } => {
                self.handle_agent_ready(
                    agent_id,
                    outgoing_tx,
                    session_id,
                    client_id,
                    cwd,
                    responder,
                )
                .await;
            }
            DispatcherMessage::AgentCapabilities { capabilities } => {
                self.capabilities = Some(*capabilities);
            }
            DispatcherMessage::AgentDisconnected { agent_id } => {
                self.handle_agent_disconnected(agent_id).await;
            }
            DispatcherMessage::FromClient {
                client_id,
                dispatch,
            } => {
                self.handle_from_client(client_id, dispatch).await;
            }
            DispatcherMessage::FromAgent { agent_id, dispatch } => {
                self.handle_from_agent(agent_id, dispatch).await;
            }
            DispatcherMessage::AgentQuiescent {
                session_id,
                generation,
            } => {
                self.handle_agent_quiescent(&session_id, generation).await;
            }
            DispatcherMessage::IdleTimeoutElapsed {
                session_id,
                generation,
            } => {
                self.handle_idle_timeout(&session_id, generation).await;
            }
            DispatcherMessage::CwdHealthCheck => {
                self.handle_cwd_health_check().await;
            }
            DispatcherMessage::ResponseSent {
                client_id,
                method,
                request_id,
                payload,
            } => {
                let session_id = self.client_to_session.get(&client_id).cloned();
                self.trace_record(NewTrace {
                    session_id,
                    dir: TraceDirection::DaemonToClient,
                    role: Some("acp-client".to_string()),
                    kind: TraceKind::Response,
                    method: Some(method),
                    request_id: Some(request_id),
                    payload,
                })
                .await;
            }
            DispatcherMessage::ModelSet {
                session_id,
                from,
                to,
            } => {
                self.trace_event(
                    Some(session_id),
                    "daemon",
                    "model_set",
                    serde_json::json!({ "from": from, "to": to }),
                )
                .await;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Client disconnect
    // -----------------------------------------------------------------------

    // ANCHOR: disconnect-and-idle
    async fn handle_client_disconnected(&mut self, client_id: ClientId) {
        self.clients.remove(&client_id);

        let Some(session_id) = self.client_to_session.remove(&client_id) else {
            self.trace_event(
                None,
                "acp-client",
                "client_disconnected",
                serde_json::json!({}),
            )
            .await;
            return;
        };
        self.trace_event(
            Some(session_id.clone()),
            "acp-client",
            "client_disconnected",
            serde_json::json!({}),
        )
        .await;
        let Some(session) = self.sessions.get_mut(&session_id) else {
            return;
        };

        session.client_ids.retain(|id| *id != client_id);
        session.generation += 1;

        if session.lifecycle_state == LifecycleState::AgentDead {
            return;
        }

        // Only start quiescence if ALL clients disconnected
        if !session.client_ids.is_empty() {
            return;
        }

        let current_gen = session.generation;
        let sid = session_id.clone();
        let tx = self.dispatcher_tx.clone();
        let quiescence_timeout = self.quiescence_timeout;

        tokio::spawn(async move {
            tokio::time::sleep(quiescence_timeout).await;
            let _ = tx.send(DispatcherMessage::AgentQuiescent {
                session_id: sid,
                generation: current_gen,
            });
        });

        self.emit(LifecycleEvent::ClientDisconnected {
            session_id: Some(session_id),
        });
    }
    // ANCHOR_END: disconnect-and-idle

    // -----------------------------------------------------------------------
    // Agent ready
    // -----------------------------------------------------------------------

    async fn handle_agent_ready(
        &mut self,
        agent_id: AgentId,
        outgoing_tx: mpsc::UnboundedSender<Dispatch>,
        session_id: SessionId,
        client_id: ClientId,
        cwd: PathBuf,
        responder: AgentReadyResponder,
    ) {
        self.agents.insert(agent_id, AgentHandle { outgoing_tx });
        self.agent_to_session.insert(agent_id, session_id.clone());
        self.trace_event(
            Some(session_id.clone()),
            "agent",
            "agent_spawned",
            serde_json::json!({ "agent_id": agent_id }),
        )
        .await;

        let is_new = !self.sessions.contains_key(&session_id);

        if let Some(session) = self.sessions.get_mut(&session_id) {
            session.agent_id = agent_id;
            session.lifecycle_state = LifecycleState::Active;
            session.respawn_attempted = false;
            if !session.client_ids.contains(&client_id) {
                session.client_ids.push(client_id);
            }
        } else {
            if let Err(e) = self.store.add_session(&session_id, &cwd).await {
                let err = agent_client_protocol::Error::internal_error().data(e.to_string());
                respond_agent_ready_error(responder, err);
                return;
            }

            let now = Utc::now();
            let record = SessionRecord {
                session_id: session_id.clone(),
                cwd,
                created_at: now,
                updated_at: now,
            };

            self.sessions.insert(
                session_id.clone(),
                Session {
                    agent_id,
                    client_ids: vec![client_id],
                    generation: 0,
                    lifecycle_state: LifecycleState::Active,
                    respawn_attempted: false,
                    record,
                },
            );
        }

        self.client_to_session.insert(client_id, session_id.clone());

        match responder {
            AgentReadyResponder::NewSession(r) => {
                let _ = r.respond(NewSessionResponse::new(session_id.clone()));
                if is_new {
                    self.trace_event(
                        Some(session_id.clone()),
                        "daemon",
                        "session_created",
                        serde_json::json!({ "session_id": session_id }),
                    )
                    .await;
                    self.emit(LifecycleEvent::SessionCreated { session_id });
                }
            }
            AgentReadyResponder::LoadSession(r) => {
                if let Err(e) = self.replay_session_to_client(&session_id, client_id).await {
                    let _ = r.respond_with_error(
                        agent_client_protocol::Error::internal_error().data(e.to_string()),
                    );
                    return;
                }
                let _ = r.respond(LoadSessionResponse::new());
                self.trace_event(
                    Some(session_id.clone()),
                    "daemon",
                    "session_loaded",
                    serde_json::json!({ "session_id": session_id }),
                )
                .await;
                self.emit(LifecycleEvent::SessionLoaded { session_id });
            }
            AgentReadyResponder::ResumeSession(r) => {
                let _ = r.respond(ResumeSessionResponse::new());
                self.trace_event(
                    Some(session_id.clone()),
                    "daemon",
                    "session_resumed",
                    serde_json::json!({ "session_id": session_id }),
                )
                .await;
                self.emit(LifecycleEvent::SessionResumed { session_id });
            }
        }
    }

    // -----------------------------------------------------------------------
    // Agent disconnected
    // -----------------------------------------------------------------------

    // ANCHOR: handle-agent-exited
    async fn handle_agent_disconnected(&mut self, agent_id: AgentId) {
        self.agents.remove(&agent_id);

        let Some(session_id) = self.agent_to_session.remove(&agent_id) else {
            return;
        };
        let Some(session) = self.sessions.get_mut(&session_id) else {
            return;
        };

        if session.lifecycle_state == LifecycleState::AgentDead {
            return;
        }

        session.lifecycle_state = LifecycleState::AgentDead;
        tracing::warn!(session_id, "agent disconnected unexpectedly");
        self.trace_event(
            Some(session_id),
            "agent",
            "agent_crashed",
            serde_json::json!({ "agent_id": agent_id }),
        )
        .await;
    }
    // ANCHOR_END: handle-agent-exited

    // -----------------------------------------------------------------------
    // FromClient routing
    // -----------------------------------------------------------------------

    // ANCHOR: dispatch-session-new
    // ANCHOR: dispatch-session-load
    async fn handle_from_client(&mut self, client_id: ClientId, dispatch: Dispatch) {
        self.trace_record_optional(self.trace_dispatch(
            self.client_to_session.get(&client_id).cloned(),
            TraceDirection::ClientToDaemon,
            "acp-client",
            &dispatch,
        ))
        .await;

        MatchDispatch::new(dispatch)
            .if_request(async |req: InitializeRequest, responder| {
                let responder = self.wrap_local_responder(client_id, responder);
                self.handle_initialize(client_id, req, responder);
                Ok(())
            })
            .await
            .if_request(async |req: ListSessionsRequest, responder| {
                let responder = self.wrap_local_responder(client_id, responder);
                self.handle_list_sessions(req, responder).await;
                Ok(())
            })
            .await
            .if_request(async |req: NewSessionRequest, responder| {
                let responder = self.wrap_local_responder(client_id, responder);
                self.handle_session_new(client_id, req, responder);
                Ok(())
            })
            .await
            .if_request(async |req: LoadSessionRequest, responder| {
                let responder = self.wrap_local_responder(client_id, responder);
                self.handle_session_load(client_id, req, responder).await;
                Ok(())
            })
            .await
            .if_request(async |req: ResumeSessionRequest, responder| {
                let responder = self.wrap_local_responder(client_id, responder);
                self.handle_session_resume(client_id, req, responder).await;
                Ok(())
            })
            .await
            .otherwise(async |dispatch| {
                self.route_to_agent(client_id, dispatch).await;
                Ok(())
            })
            .await
            .ok();
    }
    // ANCHOR_END: dispatch-session-new
    // ANCHOR_END: dispatch-session-load

    async fn route_to_agent(&self, client_id: ClientId, dispatch: Dispatch) {
        let Some(session_id) = self.client_to_session.get(&client_id) else {
            tracing::warn!(client_id, "dispatch from unrouted client");
            return;
        };
        let Some(session) = self.sessions.get(session_id) else {
            tracing::warn!(client_id, "dispatch for unknown session");
            return;
        };
        let Some(agent) = self.agents.get(&session.agent_id) else {
            tracing::warn!(client_id, "dispatch but no agent");
            return;
        };
        self.trace_record_optional(self.trace_dispatch(
            Some(session_id.clone()),
            TraceDirection::DaemonToAgent,
            "agent",
            &dispatch,
        ))
        .await;
        let _ = agent.outgoing_tx.send(dispatch);
    }

    // -----------------------------------------------------------------------
    // Initialize
    // -----------------------------------------------------------------------

    // ANCHOR: handle-initialize
    fn handle_initialize(
        &mut self,
        _client_id: ClientId,
        req: InitializeRequest,
        responder: Responder<InitializeResponse>,
    ) {
        if let Some(cached) = &self.capabilities {
            let _ = responder.respond(cached.clone());
            return;
        }

        // Cold cache: spawn a probe task. The result comes back as AgentCapabilities.
        // We store the responder and fulfill it when capabilities arrive.
        let factory = self.factory.clone();
        let dispatcher_tx = self.dispatcher_tx.clone();

        tokio::spawn(async move {
            let transport = match factory.create_transport("", std::path::Path::new("/"), &[]) {
                Ok(t) => t,
                Err(e) => {
                    let _ = responder.respond_with_error(
                        agent_client_protocol::Error::internal_error().data(e.to_string()),
                    );
                    return;
                }
            };

            let init_req = req;
            let result = Client
                .builder()
                .name("jamsession-daemon-caps")
                .connect_with(
                    transport,
                    async move |cx: agent_client_protocol::ConnectionTo<
                        agent_client_protocol::Agent,
                    >| {
                        cx.send_request(
                            InitializeRequest::new(ProtocolVersion::V1)
                                .client_capabilities(init_req.client_capabilities.clone()),
                        )
                        .block_task()
                        .await
                    },
                )
                .await;

            match result {
                Ok(response) => {
                    let _ = dispatcher_tx.send(DispatcherMessage::AgentCapabilities {
                        capabilities: Box::new(response.clone()),
                    });
                    let _ = responder.respond(response);
                }
                Err(e) => {
                    let _ = responder.respond_with_error(e);
                }
            }
        });
    }
    // ANCHOR_END: handle-initialize

    // -----------------------------------------------------------------------
    // ListSessions
    // -----------------------------------------------------------------------

    // ANCHOR: handle-session-list
    async fn handle_list_sessions(
        &self,
        req: ListSessionsRequest,
        responder: Responder<ListSessionsResponse>,
    ) {
        let sessions = match self.store.list_sessions(req.cwd.as_deref()).await {
            Ok(sessions) => sessions,
            Err(e) => {
                let _ = responder.respond_with_error(
                    agent_client_protocol::Error::internal_error().data(e.to_string()),
                );
                return;
            }
        };
        let session_infos: Vec<SessionInfo> = sessions
            .into_iter()
            .map(|s| SessionInfo::new(s.session_id, s.cwd).updated_at(s.updated_at.to_rfc3339()))
            .collect();
        let _ = responder.respond(ListSessionsResponse::new(session_infos));
    }
    // ANCHOR_END: handle-session-list

    // -----------------------------------------------------------------------
    // Session/New
    // -----------------------------------------------------------------------

    // ANCHOR: handle-session-new
    fn handle_session_new(
        &mut self,
        client_id: ClientId,
        req: NewSessionRequest,
        responder: Responder<NewSessionResponse>,
    ) {
        if !req.cwd.is_absolute() || !req.cwd.exists() {
            let _ = responder.respond_with_error(
                agent_client_protocol::Error::invalid_params()
                    .data(format!("invalid cwd: {}", req.cwd.display())),
            );
            return;
        }

        let factory = self.factory.clone();
        let dispatcher_tx = self.dispatcher_tx.clone();
        let agent_id = self.generate_agent_id();
        let send_guidelines = self.send_guidelines;
        let default_model = self.default_model.clone();

        let transport = match factory.create_transport("", &req.cwd, &req.mcp_servers) {
            Ok(t) => t,
            Err(e) => {
                let _ = responder.respond_with_error(
                    agent_client_protocol::Error::internal_error().data(e.to_string()),
                );
                return;
            }
        };

        let _ = self.tasks.spawn(async move {
            agent_pipe(
                transport,
                dispatcher_tx,
                AgentSpawnRequest {
                    client_id,
                    agent_id,
                    request: SessionRequest::New {
                        cwd: req.cwd,
                        mcp_servers: req.mcp_servers,
                    },
                    send_guidelines,
                    default_model,
                },
                AgentReadyResponder::NewSession(responder),
            )
            .await;
            Ok(())
        });
    }
    // ANCHOR_END: handle-session-new

    // -----------------------------------------------------------------------
    // Session/Load
    // -----------------------------------------------------------------------

    // ANCHOR: handle-session-load
    async fn handle_session_load(
        &mut self,
        client_id: ClientId,
        req: LoadSessionRequest,
        responder: Responder<LoadSessionResponse>,
    ) {
        let session_id = req.session_id.0.to_string();

        let Some(session) = self.sessions.get(&session_id) else {
            let _ = responder.respond_with_error(
                agent_client_protocol::Error::invalid_params()
                    .data(format!("session not found: {session_id}")),
            );
            return;
        };

        let is_dead = session.lifecycle_state == LifecycleState::AgentDead;
        let cwd = session.record.cwd.clone();

        if is_dead {
            self.spawn_resumed_agent(
                client_id,
                session_id,
                cwd,
                req.mcp_servers,
                AgentReadyResponder::LoadSession(responder),
            );
        } else {
            if let Err(e) = self.replay_session_to_client(&session_id, client_id).await {
                let _ = responder.respond_with_error(
                    agent_client_protocol::Error::internal_error().data(e.to_string()),
                );
                return;
            }

            self.client_to_session.insert(client_id, session_id.clone());
            if let Some(session) = self.sessions.get_mut(&session_id) {
                if !session.client_ids.contains(&client_id) {
                    session.client_ids.push(client_id);
                }
                session.generation += 1;
            }

            let _ = responder.respond(LoadSessionResponse::new());
            self.trace_event(
                Some(session_id.clone()),
                "daemon",
                "session_loaded",
                serde_json::json!({ "session_id": session_id }),
            )
            .await;
            self.emit(LifecycleEvent::SessionLoaded { session_id });
        }
    }
    // ANCHOR_END: handle-session-load

    // -----------------------------------------------------------------------
    // Session/Resume
    // -----------------------------------------------------------------------

    async fn handle_session_resume(
        &mut self,
        client_id: ClientId,
        req: ResumeSessionRequest,
        responder: Responder<ResumeSessionResponse>,
    ) {
        let session_id = req.session_id.0.to_string();

        let Some(session) = self.sessions.get(&session_id) else {
            let _ = responder.respond_with_error(
                agent_client_protocol::Error::invalid_params()
                    .data(format!("session not found: {session_id}")),
            );
            return;
        };

        let is_dead = session.lifecycle_state == LifecycleState::AgentDead;
        let cwd = session.record.cwd.clone();

        if is_dead {
            self.spawn_resumed_agent(
                client_id,
                session_id,
                cwd,
                req.mcp_servers,
                AgentReadyResponder::ResumeSession(responder),
            );
        } else {
            // Agent alive: just wire client to session (no replay for resume)
            self.client_to_session.insert(client_id, session_id.clone());
            if let Some(session) = self.sessions.get_mut(&session_id) {
                if !session.client_ids.contains(&client_id) {
                    session.client_ids.push(client_id);
                }
                session.generation += 1;
            }

            let _ = responder.respond(ResumeSessionResponse::new());
            self.trace_event(
                Some(session_id.clone()),
                "daemon",
                "session_resumed",
                serde_json::json!({ "session_id": session_id }),
            )
            .await;
            self.emit(LifecycleEvent::SessionResumed { session_id });
        }
    }

    fn spawn_resumed_agent(
        &mut self,
        client_id: ClientId,
        session_id: SessionId,
        cwd: PathBuf,
        mcp_servers: Vec<McpServer>,
        responder: AgentReadyResponder,
    ) {
        let factory = self.factory.clone();
        let dispatcher_tx = self.dispatcher_tx.clone();
        let agent_id = self.generate_agent_id();
        let default_model = self.default_model.clone();

        let transport = match factory.create_transport(&session_id, &cwd, &mcp_servers) {
            Ok(t) => t,
            Err(e) => {
                respond_agent_ready_error(
                    responder,
                    agent_client_protocol::Error::internal_error().data(e.to_string()),
                );
                return;
            }
        };

        let _ = self.tasks.spawn(async move {
            agent_pipe(
                transport,
                dispatcher_tx,
                AgentSpawnRequest {
                    client_id,
                    agent_id,
                    request: SessionRequest::Resume {
                        session_id,
                        cwd,
                        mcp_servers,
                    },
                    send_guidelines: false,
                    default_model,
                },
                responder,
            )
            .await;
            Ok(())
        });
    }

    // -----------------------------------------------------------------------
    // FromAgent routing
    // -----------------------------------------------------------------------

    // ANCHOR: route-messages
    async fn handle_from_agent(&mut self, agent_id: AgentId, dispatch: Dispatch) {
        let Some(session_id) = self.agent_to_session.get(&agent_id).cloned() else {
            tracing::warn!(agent_id, "dispatch from unknown agent");
            return;
        };
        self.trace_record_optional(self.trace_dispatch(
            Some(session_id.clone()),
            TraceDirection::AgentToDaemon,
            "agent",
            &dispatch,
        ))
        .await;
        let Some(session) = self.sessions.get_mut(&session_id) else {
            return;
        };

        if let Dispatch::Notification(ref notif) = dispatch
            && let Ok(value) = serde_json::to_value(notif)
            && let Err(e) = self.store.append_message(&session_id, &value).await
        {
            tracing::error!(session_id, error = %e, "failed to persist agent notification");
        }

        session.generation += 1;

        if let Some(&cid) = session.client_ids.last()
            && let Some(client) = self.clients.get(&cid)
        {
            self.trace_record_optional(self.trace_dispatch(
                Some(session_id),
                TraceDirection::DaemonToClient,
                "acp-client",
                &dispatch,
            ))
            .await;
            let _ = client.outgoing_tx.send(dispatch);
        }
    }
    // ANCHOR_END: route-messages

    // -----------------------------------------------------------------------
    // Timers
    // -----------------------------------------------------------------------

    async fn handle_agent_quiescent(&mut self, session_id: &str, generation: u64) {
        let Some(session) = self.sessions.get_mut(session_id) else {
            return;
        };
        if session.generation != generation {
            return;
        }

        session.lifecycle_state = LifecycleState::Quiescent;

        self.trace_event(
            Some(session_id.to_string()),
            "agent",
            "agent_quiescent",
            serde_json::json!({}),
        )
        .await;
        self.emit(LifecycleEvent::AgentQuiescent {
            session_id: session_id.to_string(),
        });

        let sid = session_id.to_string();
        let tx = self.dispatcher_tx.clone();
        let idle_timeout = self.idle_timeout;

        tokio::spawn(async move {
            tokio::time::sleep(idle_timeout).await;
            let _ = tx.send(DispatcherMessage::IdleTimeoutElapsed {
                session_id: sid,
                generation,
            });
        });
    }

    async fn handle_idle_timeout(&mut self, session_id: &str, generation: u64) {
        let Some(session) = self.sessions.get_mut(session_id) else {
            return;
        };
        if session.generation != generation {
            return;
        }

        // Kill agent by dropping its handle
        let agent_id = session.agent_id;
        self.agents.remove(&agent_id);
        self.agent_to_session.remove(&agent_id);

        session.lifecycle_state = LifecycleState::AgentDead;

        tracing::info!(session_id, "agent killed due to idle timeout");
        self.trace_event(
            Some(session_id.to_string()),
            "agent",
            "agent_killed_idle",
            serde_json::json!({}),
        )
        .await;
        self.emit(LifecycleEvent::AgentKilledIdle {
            session_id: session_id.to_string(),
        });
    }

    // -----------------------------------------------------------------------
    // CWD health check
    // -----------------------------------------------------------------------

    // ANCHOR: cwd-health-check
    async fn handle_cwd_health_check(&mut self) {
        let to_remove: Vec<SessionId> = self
            .sessions
            .iter()
            .filter(|(_, s)| !s.record.cwd.exists())
            .map(|(id, _)| id.clone())
            .collect();

        for sid in &to_remove {
            if let Some(session) = self.sessions.remove(sid) {
                self.agents.remove(&session.agent_id);
                self.agent_to_session.remove(&session.agent_id);
                for &cid in &session.client_ids {
                    self.client_to_session.remove(&cid);
                }
            }
            if let Err(e) = self.store.remove_session(sid).await {
                tracing::error!(session_id = sid.as_str(), error = %e, "failed to remove session");
            }
            tracing::info!(session_id = sid.as_str(), "session removed: cwd deleted");
        }
    }
    // ANCHOR_END: cwd-health-check

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn emit(&self, event: LifecycleEvent) {
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(event);
        }
    }

    fn wrap_local_responder<T>(&self, client_id: ClientId, responder: Responder<T>) -> Responder<T>
    where
        T: JsonRpcResponse,
    {
        if !self.trace {
            return responder;
        }

        let dispatcher_tx = self.dispatcher_tx.clone();
        let request_id = json_id_to_string(&responder.id());
        responder.wrap_params(
            move |method, result: Result<T, agent_client_protocol::Error>| {
                let payload = match &result {
                    Ok(value) => match value.clone().into_json(method) {
                        Ok(result) => serde_json::json!({ "result": result }),
                        Err(error) => serde_json::json!({ "error": error.to_string() }),
                    },
                    Err(error) => serde_json::json!({ "error": error.to_string() }),
                };
                let _ = dispatcher_tx.send(DispatcherMessage::ResponseSent {
                    client_id,
                    method: method.to_string(),
                    request_id,
                    payload,
                });
                result
            },
        )
    }

    fn trace_dispatch(
        &self,
        session_id: Option<String>,
        dir: TraceDirection,
        role: &str,
        dispatch: &Dispatch,
    ) -> Option<NewTrace> {
        if !self.trace {
            return None;
        }

        let kind = match dispatch {
            Dispatch::Request(_, _) => TraceKind::Request,
            Dispatch::Notification(_) => TraceKind::Notification,
            Dispatch::Response(_, _) => TraceKind::Response,
        };
        let payload = match dispatch {
            Dispatch::Request(msg, _) | Dispatch::Notification(msg) => msg.params.clone(),
            Dispatch::Response(result, _) => match result {
                Ok(value) => serde_json::json!({ "result": value }),
                Err(error) => serde_json::json!({ "error": error.to_string() }),
            },
        };
        Some(NewTrace {
            session_id,
            dir,
            role: Some(role.to_string()),
            kind,
            method: Some(dispatch.method().to_string()),
            request_id: dispatch.id().map(|id| json_id_to_string(&id)),
            payload,
        })
    }

    async fn trace_event(
        &self,
        session_id: Option<String>,
        role: &str,
        method: &str,
        payload: serde_json::Value,
    ) {
        self.trace_record(NewTrace {
            session_id,
            dir: TraceDirection::Internal,
            role: Some(role.to_string()),
            kind: TraceKind::Event,
            method: Some(method.to_string()),
            request_id: None,
            payload,
        })
        .await;
    }

    async fn trace_record_optional(&self, trace: Option<NewTrace>) {
        if let Some(trace) = trace {
            self.trace_record(trace).await;
        }
    }

    async fn trace_record(&self, trace: NewTrace) {
        if !self.trace {
            return;
        }
        if let Err(e) = self.store.record_trace(trace).await {
            tracing::error!(error = %e, "failed to record trace");
        }
    }

    async fn replay_session_to_client(
        &self,
        session_id: &str,
        client_id: ClientId,
    ) -> crate::error::Result<()> {
        let Some(client) = self.clients.get(&client_id) else {
            return Ok(());
        };

        for msg in self.store.messages_for_session(session_id).await? {
            if let Ok(untyped) =
                serde_json::from_value::<agent_client_protocol::UntypedMessage>(msg)
            {
                let _ = client.outgoing_tx.send(Dispatch::Notification(untyped));
            }
        }

        Ok(())
    }

    fn generate_agent_id(&mut self) -> AgentId {
        let id = self.next_agent_id;
        self.next_agent_id += 1;
        id
    }
}

fn json_id_to_string(id: &serde_json::Value) -> String {
    match id {
        serde_json::Value::String(s) => s.clone(),
        _ => id.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Client Pipe
// ---------------------------------------------------------------------------

// ANCHOR: handle-client
pub(super) async fn client_pipe(
    stream: UnixStream,
    client_id: ClientId,
    dispatcher_tx: mpsc::UnboundedSender<DispatcherMessage>,
) {
    let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel::<Dispatch>();

    let _ = dispatcher_tx.send(DispatcherMessage::ClientRegistered {
        client_id,
        outgoing_tx,
    });

    let (read_half, write_half) = stream.into_split();
    let transport = ByteStreams::new(write_half.compat_write(), read_half.compat());
    // Workaround for https://github.com/agentclientprotocol/rust-sdk/issues/223
    let (transport, eof_rx) = crate::eof_signal::EofSignalingTransport::wrap(transport);

    let forwarder_tx = dispatcher_tx.clone();
    let result =
        Agent
            .builder()
            .name("jamsession-daemon")
            .on_receive_dispatch(
                async move |dispatch: Dispatch,
                            _cx: agent_client_protocol::ConnectionTo<
                    agent_client_protocol::Client,
                >| {
                    let _ = forwarder_tx.send(DispatcherMessage::FromClient {
                        client_id,
                        dispatch,
                    });
                    Ok(Handled::Yes)
                },
                agent_client_protocol::on_receive_dispatch!(),
            )
            .connect_with(transport, async move |cx| {
                let eof_fut = Box::pin(async {
                    let _ = eof_rx.await;
                });
                let mut outgoing =
                    std::pin::pin!(UnboundedReceiverStream::new(outgoing_rx).take_until(eof_fut));
                while let Some(dispatch) = outgoing.next().await {
                    cx.send_proxied_message(dispatch)?;
                }
                Ok(())
            })
            .await;

    // ANCHOR: client-disconnect
    if let Err(e) = result {
        tracing::debug!(client_id, error = %e, "client pipe ended");
    }

    let _ = dispatcher_tx.send(DispatcherMessage::ClientDisconnected { client_id });
    // ANCHOR_END: client-disconnect
}
// ANCHOR_END: handle-client

// ---------------------------------------------------------------------------
// Agent Pipe
// ---------------------------------------------------------------------------

#[derive(Clone)]
enum SessionRequest {
    New {
        cwd: PathBuf,
        mcp_servers: Vec<agent_client_protocol::schema::McpServer>,
    },
    Resume {
        session_id: String,
        cwd: PathBuf,
        mcp_servers: Vec<agent_client_protocol::schema::McpServer>,
    },
}

struct AgentSpawnRequest {
    client_id: ClientId,
    agent_id: AgentId,
    request: SessionRequest,
    send_guidelines: bool,
    default_model: Option<String>,
}

async fn agent_pipe(
    transport: DynConnectTo<Client>,
    dispatcher_tx: mpsc::UnboundedSender<DispatcherMessage>,
    spawn_request: AgentSpawnRequest,
    responder: AgentReadyResponder,
) {
    let agent_id = spawn_request.agent_id;
    let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel::<Dispatch>();
    let responder_slot: Arc<std::sync::Mutex<Option<AgentReadyResponder>>> =
        Arc::new(std::sync::Mutex::new(Some(responder)));

    // Workaround for https://github.com/agentclientprotocol/rust-sdk/issues/223
    let (transport, eof_rx) = crate::eof_signal::EofSignalingTransport::wrap(transport);

    let result = Client
        .builder()
        .name("jamsession-daemon-agent")
        .connect_with(transport, {
            let dispatcher_tx = dispatcher_tx.clone();
            let responder_slot = responder_slot.clone();
            async move |cx: agent_client_protocol::ConnectionTo<agent_client_protocol::Agent>| {
                cx.send_request(InitializeRequest::new(ProtocolVersion::V1))
                    .block_task()
                    .await?;

                match spawn_request.request {
                    SessionRequest::New {
                        ref cwd,
                        ref mcp_servers,
                    } => {
                        let resp = cx
                            .send_request(
                                NewSessionRequest::new(cwd).mcp_servers(mcp_servers.clone()),
                            )
                            .block_task()
                            .await?;

                        let session_id = resp.session_id.0.to_string();

                        if let Some(ref desired_model) = spawn_request.default_model {
                            set_model_config_option(
                                &cx,
                                dispatcher_tx.clone(),
                                &session_id,
                                desired_model,
                                resp.config_options.as_deref(),
                            )
                            .await;
                        }

                        if spawn_request.send_guidelines {
                            use agent_client_protocol::schema::{
                                ContentBlock, PromptRequest, TextContent,
                            };
                            static GUIDELINES: &str = include_str!("guidelines.md");
                            cx.send_request(PromptRequest::new(
                                AcpSessionId::new(session_id.as_str()),
                                vec![ContentBlock::Text(TextContent::new(GUIDELINES))],
                            ))
                            .block_task()
                            .await?;
                        }

                        cx.add_dynamic_handler(AgentDispatchForwarder {
                            agent_id,
                            dispatcher_tx: dispatcher_tx.clone(),
                        })
                        .map_err(|e| {
                            agent_client_protocol::Error::internal_error()
                                .data(format!("forwarder: {e}"))
                        })?
                        .run_indefinitely();

                        let responder = responder_slot.lock().unwrap().take().unwrap();
                        let _ = dispatcher_tx.send(DispatcherMessage::AgentReady {
                            agent_id,
                            outgoing_tx,
                            session_id,
                            client_id: spawn_request.client_id,
                            cwd: cwd.clone(),
                            responder,
                        });
                    }
                    SessionRequest::Resume {
                        ref session_id,
                        ref cwd,
                        ref mcp_servers,
                    } => {
                        cx.send_request(
                            ResumeSessionRequest::new(AcpSessionId::new(session_id.as_str()), cwd)
                                .mcp_servers(mcp_servers.clone()),
                        )
                        .block_task()
                        .await?;

                        cx.add_dynamic_handler(AgentDispatchForwarder {
                            agent_id,
                            dispatcher_tx: dispatcher_tx.clone(),
                        })
                        .map_err(|e| {
                            agent_client_protocol::Error::internal_error()
                                .data(format!("forwarder: {e}"))
                        })?
                        .run_indefinitely();

                        let responder = responder_slot.lock().unwrap().take().unwrap();
                        let _ = dispatcher_tx.send(DispatcherMessage::AgentReady {
                            agent_id,
                            outgoing_tx,
                            session_id: session_id.clone(),
                            client_id: spawn_request.client_id,
                            cwd: cwd.clone(),
                            responder,
                        });
                    }
                }

                let eof_fut = Box::pin(async {
                    let _ = eof_rx.await;
                });
                let mut outgoing =
                    std::pin::pin!(UnboundedReceiverStream::new(outgoing_rx).take_until(eof_fut));
                while let Some(dispatch) = outgoing.next().await {
                    cx.send_proxied_message(dispatch)?;
                }
                Ok(())
            }
        })
        .await;

    if let Err(e) = result {
        tracing::error!(agent_id, error = %e, "agent pipe error");
        if let Some(responder) = responder_slot.lock().unwrap().take() {
            let err = agent_client_protocol::Error::internal_error().data(e.to_string());
            respond_agent_ready_error(responder, err);
        }
    }

    let _ = dispatcher_tx.send(DispatcherMessage::AgentDisconnected { agent_id });
}

// ---------------------------------------------------------------------------
// Model configuration
// ---------------------------------------------------------------------------

use agent_client_protocol::schema::SessionConfigKind;

async fn set_model_config_option(
    cx: &agent_client_protocol::ConnectionTo<agent_client_protocol::Agent>,
    dispatcher_tx: mpsc::UnboundedSender<DispatcherMessage>,
    session_id: &str,
    desired_model: &str,
    config_options: Option<&[agent_client_protocol::schema::SessionConfigOption]>,
) {
    let Some(options) = config_options else {
        tracing::debug!("no config options returned by agent, skipping model set");
        return;
    };

    let model_option = options
        .iter()
        .find(|opt| opt.category == Some(SessionConfigOptionCategory::Model));

    let Some(model_option) = model_option else {
        tracing::debug!("no model config option found, skipping model set");
        return;
    };

    let SessionConfigKind::Select(ref select) = model_option.kind else {
        tracing::debug!("model config option is not a select type, skipping");
        return;
    };

    if &*select.current_value.0 == desired_model {
        tracing::debug!(model = desired_model, "model already set to desired value");
        return;
    }
    let current_model = select.current_value.0.to_string();

    tracing::info!(
        from = %current_model,
        to = desired_model,
        "setting model via session/set_config_option"
    );

    use agent_client_protocol::schema::SessionConfigValueId;
    let req = SetSessionConfigOptionRequest::new(
        AcpSessionId::new(session_id),
        model_option.id.clone(),
        SessionConfigValueId::new(desired_model),
    );

    match cx.send_request(req).block_task().await {
        Ok(_resp) => {
            tracing::info!(model = desired_model, "model set successfully");
            let _ = dispatcher_tx.send(DispatcherMessage::ModelSet {
                session_id: session_id.to_string(),
                from: current_model,
                to: desired_model.to_string(),
            });
        }
        Err(e) => {
            tracing::warn!(model = desired_model, error = %e, "failed to set model");
        }
    }
}

// ---------------------------------------------------------------------------
// AgentDispatchForwarder
// ---------------------------------------------------------------------------

struct AgentDispatchForwarder {
    agent_id: AgentId,
    dispatcher_tx: mpsc::UnboundedSender<DispatcherMessage>,
}

impl HandleDispatchFrom<agent_client_protocol::Agent> for AgentDispatchForwarder {
    async fn handle_dispatch_from(
        &mut self,
        message: Dispatch,
        _cx: agent_client_protocol::ConnectionTo<agent_client_protocol::Agent>,
    ) -> agent_client_protocol::schema::Result<Handled<Dispatch>> {
        let _ = self.dispatcher_tx.send(DispatcherMessage::FromAgent {
            agent_id: self.agent_id,
            dispatch: message,
        });
        Ok(Handled::Yes)
    }

    fn describe_chain(&self) -> impl std::fmt::Debug {
        "AgentDispatchForwarder"
    }
}
