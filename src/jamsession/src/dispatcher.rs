use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use agent_client_protocol::schema::{
    InitializeRequest, InitializeResponse, ListSessionsRequest, ListSessionsResponse,
    LoadSessionRequest, LoadSessionResponse, NewSessionRequest, NewSessionResponse,
    ProtocolVersion, ResumeSessionRequest, ResumeSessionResponse, SessionId as AcpSessionId,
    SessionInfo,
};
use agent_client_protocol::util::MatchDispatch;
use agent_client_protocol::{
    Agent, ByteStreams, Client, Dispatch, DynConnectTo, HandleDispatchFrom, Handled, Responder,
};
use chrono::Utc;
use futures::StreamExt;
use scope_tasks::TaskSpawner;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::agent::AgentFactory;
use crate::session::{LifecycleEvent, LifecycleEventSender};
use crate::state::{DaemonState, SessionRecord};

// ---------------------------------------------------------------------------
// IDs
// ---------------------------------------------------------------------------

type ClientId = u64;
type AgentId = u64;
type SessionId = String;

// ---------------------------------------------------------------------------
// DispatcherMessage
// ---------------------------------------------------------------------------

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
        replay_notifications: Vec<serde_json::Value>,
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
}

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
    buffer: Vec<serde_json::Value>,
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
enum AgentReadyResponder {
    NewSession(Responder<NewSessionResponse>),
    LoadSession(Responder<LoadSessionResponse>),
    ResumeSession(Responder<ResumeSessionResponse>),
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
    state: DaemonState,
    state_path: PathBuf,
    capabilities: Option<InitializeResponse>,
    factory: Arc<dyn AgentFactory>,
    idle_timeout: std::time::Duration,
    quiescence_timeout: std::time::Duration,
    send_guidelines: bool,
    event_tx: Option<LifecycleEventSender>,
    dispatcher_tx: mpsc::UnboundedSender<DispatcherMessage>,
    next_agent_id: u64,
}

impl<'scope> Dispatcher<'scope> {
    #[expect(clippy::too_many_arguments)]
    pub(super) fn new(
        tasks: TaskSpawner<'scope, crate::error::Error>,
        state: DaemonState,
        state_path: PathBuf,
        factory: Arc<dyn AgentFactory>,
        idle_timeout: std::time::Duration,
        quiescence_timeout: std::time::Duration,
        send_guidelines: bool,
        event_tx: Option<LifecycleEventSender>,
        dispatcher_tx: mpsc::UnboundedSender<DispatcherMessage>,
    ) -> Self {
        let mut dispatcher = Self {
            tasks,
            clients: HashMap::new(),
            agents: HashMap::new(),
            sessions: HashMap::new(),
            client_to_session: HashMap::new(),
            agent_to_session: HashMap::new(),
            state: state.clone(),
            state_path,
            capabilities: None,
            factory,
            idle_timeout,
            quiescence_timeout,
            send_guidelines,
            event_tx,
            dispatcher_tx,
            next_agent_id: 1,
        };
        dispatcher.rehydrate_from_state(&state);
        dispatcher
    }

    fn rehydrate_from_state(&mut self, state: &DaemonState) {
        for record in &state.sessions {
            if !self.sessions.contains_key(&record.session_id) {
                self.sessions.insert(
                    record.session_id.clone(),
                    Session {
                        agent_id: 0,
                        client_ids: Vec::new(),
                        buffer: Vec::new(),
                        generation: 0,
                        lifecycle_state: LifecycleState::AgentDead,
                        respawn_attempted: false,
                        record: record.clone(),
                    },
                );
            }
        }
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
                self.emit(LifecycleEvent::ClientConnected);
            }
            DispatcherMessage::ClientDisconnected { client_id } => {
                self.handle_client_disconnected(client_id);
            }
            DispatcherMessage::AgentReady {
                agent_id,
                outgoing_tx,
                session_id,
                client_id,
                cwd,
                replay_notifications,
                responder,
            } => {
                self.handle_agent_ready(
                    agent_id,
                    outgoing_tx,
                    session_id,
                    client_id,
                    cwd,
                    replay_notifications,
                    responder,
                );
            }
            DispatcherMessage::AgentCapabilities { capabilities } => {
                self.capabilities = Some(*capabilities);
            }
            DispatcherMessage::AgentDisconnected { agent_id } => {
                self.handle_agent_disconnected(agent_id);
            }
            DispatcherMessage::FromClient {
                client_id,
                dispatch,
            } => {
                self.handle_from_client(client_id, dispatch).await;
            }
            DispatcherMessage::FromAgent { agent_id, dispatch } => {
                self.handle_from_agent(agent_id, dispatch);
            }
            DispatcherMessage::AgentQuiescent {
                session_id,
                generation,
            } => {
                self.handle_agent_quiescent(&session_id, generation);
            }
            DispatcherMessage::IdleTimeoutElapsed {
                session_id,
                generation,
            } => {
                self.handle_idle_timeout(&session_id, generation);
            }
            DispatcherMessage::CwdHealthCheck => {
                self.handle_cwd_health_check();
            }
        }
    }

    // -----------------------------------------------------------------------
    // Client disconnect
    // -----------------------------------------------------------------------

    fn handle_client_disconnected(&mut self, client_id: ClientId) {
        self.clients.remove(&client_id);

        let Some(session_id) = self.client_to_session.remove(&client_id) else {
            return;
        };
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

    // -----------------------------------------------------------------------
    // Agent ready
    // -----------------------------------------------------------------------

    #[expect(clippy::too_many_arguments)]
    fn handle_agent_ready(
        &mut self,
        agent_id: AgentId,
        outgoing_tx: mpsc::UnboundedSender<Dispatch>,
        session_id: SessionId,
        client_id: ClientId,
        cwd: PathBuf,
        replay_notifications: Vec<serde_json::Value>,
        responder: AgentReadyResponder,
    ) {
        self.agents.insert(agent_id, AgentHandle { outgoing_tx });
        self.agent_to_session.insert(agent_id, session_id.clone());

        let is_new = !self.sessions.contains_key(&session_id);

        if let Some(session) = self.sessions.get_mut(&session_id) {
            session.agent_id = agent_id;
            session.lifecycle_state = LifecycleState::Active;
            session.respawn_attempted = false;
            if !session.client_ids.contains(&client_id) {
                session.client_ids.push(client_id);
            }
            if !replay_notifications.is_empty() {
                session.buffer = replay_notifications;
            }
        } else {
            let record = SessionRecord {
                session_id: session_id.clone(),
                cwd,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            };
            self.state.add_session(record.clone());
            let _ = self.state.save(&self.state_path);

            self.sessions.insert(
                session_id.clone(),
                Session {
                    agent_id,
                    client_ids: vec![client_id],
                    buffer: replay_notifications,
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
                    self.emit(LifecycleEvent::SessionCreated { session_id });
                }
            }
            AgentReadyResponder::LoadSession(r) => {
                if let Some(session) = self.sessions.get(&session_id)
                    && let Some(client) = self.clients.get(&client_id)
                {
                    for msg in &session.buffer {
                        if let Ok(untyped) = serde_json::from_value::<
                            agent_client_protocol::UntypedMessage,
                        >(msg.clone())
                        {
                            let _ = client.outgoing_tx.send(Dispatch::Notification(untyped));
                        }
                    }
                }
                let _ = r.respond(LoadSessionResponse::new());
                self.emit(LifecycleEvent::SessionLoaded { session_id });
            }
            AgentReadyResponder::ResumeSession(r) => {
                let _ = r.respond(ResumeSessionResponse::new());
                self.emit(LifecycleEvent::SessionResumed { session_id });
            }
        }
    }

    // -----------------------------------------------------------------------
    // Agent disconnected
    // -----------------------------------------------------------------------

    fn handle_agent_disconnected(&mut self, agent_id: AgentId) {
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
        session.buffer.clear();
        tracing::warn!(session_id, "agent disconnected unexpectedly");
    }

    // -----------------------------------------------------------------------
    // FromClient routing
    // -----------------------------------------------------------------------

    async fn handle_from_client(&mut self, client_id: ClientId, dispatch: Dispatch) {
        MatchDispatch::new(dispatch)
            .if_request(async |req: InitializeRequest, responder| {
                self.handle_initialize(client_id, req, responder);
                Ok(())
            })
            .await
            .if_request(async |req: ListSessionsRequest, responder| {
                self.handle_list_sessions(req, responder);
                Ok(())
            })
            .await
            .if_request(async |req: NewSessionRequest, responder| {
                self.handle_session_new(client_id, req, responder);
                Ok(())
            })
            .await
            .if_request(async |req: LoadSessionRequest, responder| {
                self.handle_session_load(client_id, req, responder);
                Ok(())
            })
            .await
            .if_request(async |req: ResumeSessionRequest, responder| {
                self.handle_session_resume(client_id, req, responder);
                Ok(())
            })
            .await
            .otherwise(async |dispatch| {
                self.route_to_agent(client_id, dispatch);
                Ok(())
            })
            .await
            .ok();
    }

    fn route_to_agent(&self, client_id: ClientId, dispatch: Dispatch) {
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
        let _ = agent.outgoing_tx.send(dispatch);
    }

    // -----------------------------------------------------------------------
    // Initialize
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // ListSessions
    // -----------------------------------------------------------------------

    fn handle_list_sessions(
        &self,
        req: ListSessionsRequest,
        responder: Responder<ListSessionsResponse>,
    ) {
        let sessions = self.state.list_sessions_by_cwd(req.cwd.as_deref());
        let session_infos: Vec<SessionInfo> = sessions
            .into_iter()
            .map(|s| {
                SessionInfo::new(s.session_id.clone(), s.cwd.clone())
                    .updated_at(s.updated_at.to_rfc3339())
            })
            .collect();
        let _ = responder.respond(ListSessionsResponse::new(session_infos));
    }

    // -----------------------------------------------------------------------
    // Session/New
    // -----------------------------------------------------------------------

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
                },
                AgentReadyResponder::NewSession(responder),
            )
            .await;
            Ok(())
        });
    }

    // -----------------------------------------------------------------------
    // Session/Load
    // -----------------------------------------------------------------------

    fn handle_session_load(
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
            let factory = self.factory.clone();
            let dispatcher_tx = self.dispatcher_tx.clone();
            let agent_id = self.generate_agent_id();

            let transport = match factory.create_transport(&session_id, &cwd, &req.mcp_servers) {
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
                        request: SessionRequest::Load {
                            session_id,
                            cwd,
                            mcp_servers: req.mcp_servers,
                        },
                        send_guidelines: false,
                    },
                    AgentReadyResponder::LoadSession(responder),
                )
                .await;
                Ok(())
            });
        } else {
            // Agent alive: replay buffer to new client, respond immediately
            if let Some(client) = self.clients.get(&client_id) {
                for msg in &session.buffer {
                    if let Ok(untyped) =
                        serde_json::from_value::<agent_client_protocol::UntypedMessage>(msg.clone())
                    {
                        let _ = client.outgoing_tx.send(Dispatch::Notification(untyped));
                    }
                }
            }

            // Wire client to this session
            self.client_to_session.insert(client_id, session_id.clone());
            if let Some(session) = self.sessions.get_mut(&session_id) {
                if !session.client_ids.contains(&client_id) {
                    session.client_ids.push(client_id);
                }
                session.generation += 1;
            }

            let _ = responder.respond(LoadSessionResponse::new());
        }
    }

    // -----------------------------------------------------------------------
    // Session/Resume
    // -----------------------------------------------------------------------

    fn handle_session_resume(
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
            let factory = self.factory.clone();
            let dispatcher_tx = self.dispatcher_tx.clone();
            let agent_id = self.generate_agent_id();

            let transport = match factory.create_transport(&session_id, &cwd, &req.mcp_servers) {
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
                        request: SessionRequest::Load {
                            session_id,
                            cwd,
                            mcp_servers: req.mcp_servers,
                        },
                        send_guidelines: false,
                    },
                    AgentReadyResponder::ResumeSession(responder),
                )
                .await;
                Ok(())
            });
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
        }
    }

    // -----------------------------------------------------------------------
    // FromAgent routing
    // -----------------------------------------------------------------------

    fn handle_from_agent(&mut self, agent_id: AgentId, dispatch: Dispatch) {
        let Some(session_id) = self.agent_to_session.get(&agent_id).cloned() else {
            tracing::warn!(agent_id, "dispatch from unknown agent");
            return;
        };
        let Some(session) = self.sessions.get_mut(&session_id) else {
            return;
        };

        if let Dispatch::Notification(ref notif) = dispatch
            && let Ok(value) = serde_json::to_value(notif)
        {
            session.buffer.push(value);
        }

        session.generation += 1;

        if let Some(&cid) = session.client_ids.last()
            && let Some(client) = self.clients.get(&cid)
        {
            let _ = client.outgoing_tx.send(dispatch);
        }
    }

    // -----------------------------------------------------------------------
    // Timers
    // -----------------------------------------------------------------------

    fn handle_agent_quiescent(&mut self, session_id: &str, generation: u64) {
        let Some(session) = self.sessions.get_mut(session_id) else {
            return;
        };
        if session.generation != generation {
            return;
        }

        session.lifecycle_state = LifecycleState::Quiescent;

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

    fn handle_idle_timeout(&mut self, session_id: &str, generation: u64) {
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
        session.buffer.clear();

        tracing::info!(session_id, "agent killed due to idle timeout");
        self.emit(LifecycleEvent::AgentKilledIdle {
            session_id: session_id.to_string(),
        });
    }

    // -----------------------------------------------------------------------
    // CWD health check
    // -----------------------------------------------------------------------

    fn handle_cwd_health_check(&mut self) {
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
            self.state.remove_session(sid);
            tracing::info!(session_id = sid.as_str(), "session removed: cwd deleted");
        }

        if !to_remove.is_empty() {
            let _ = self.state.save(&self.state_path);
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn emit(&self, event: LifecycleEvent) {
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(event);
        }
    }

    fn generate_agent_id(&mut self) -> AgentId {
        let id = self.next_agent_id;
        self.next_agent_id += 1;
        id
    }
}

// ---------------------------------------------------------------------------
// Client Pipe
// ---------------------------------------------------------------------------

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

    if let Err(e) = result {
        tracing::debug!(client_id, error = %e, "client pipe ended");
    }

    let _ = dispatcher_tx.send(DispatcherMessage::ClientDisconnected { client_id });
}

// ---------------------------------------------------------------------------
// Agent Pipe
// ---------------------------------------------------------------------------

#[derive(Clone)]
enum SessionRequest {
    New {
        cwd: PathBuf,
        mcp_servers: Vec<agent_client_protocol::schema::McpServer>,
    },
    Load {
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
                            replay_notifications: Vec::new(),
                            responder,
                        });
                    }
                    SessionRequest::Load {
                        ref session_id,
                        ref cwd,
                        ref mcp_servers,
                    } => {
                        let replay_buffer: Arc<std::sync::Mutex<Vec<serde_json::Value>>> =
                            Arc::new(std::sync::Mutex::new(Vec::new()));
                        cx.add_dynamic_handler(ReplayCapture {
                            buffer: replay_buffer.clone(),
                        })
                        .map_err(|e| {
                            agent_client_protocol::Error::internal_error()
                                .data(format!("replay capture: {e}"))
                        })?
                        .run_indefinitely();

                        cx.send_request(
                            LoadSessionRequest::new(AcpSessionId::new(session_id.as_str()), cwd)
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

                        let replay = replay_buffer.lock().unwrap().clone();
                        let responder = responder_slot.lock().unwrap().take().unwrap();
                        let _ = dispatcher_tx.send(DispatcherMessage::AgentReady {
                            agent_id,
                            outgoing_tx,
                            session_id: session_id.clone(),
                            client_id: spawn_request.client_id,
                            cwd: cwd.clone(),
                            replay_notifications: replay,
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
    }

    let _ = dispatcher_tx.send(DispatcherMessage::AgentDisconnected { agent_id });
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

// ---------------------------------------------------------------------------
// ReplayCapture
// ---------------------------------------------------------------------------

struct ReplayCapture {
    buffer: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
}

impl HandleDispatchFrom<agent_client_protocol::Agent> for ReplayCapture {
    async fn handle_dispatch_from(
        &mut self,
        message: Dispatch,
        _cx: agent_client_protocol::ConnectionTo<agent_client_protocol::Agent>,
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
