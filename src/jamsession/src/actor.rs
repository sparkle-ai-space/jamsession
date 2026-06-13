use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use agent_client_protocol::schema::{
    InitializeRequest, InitializeResponse, ListSessionsRequest, ListSessionsResponse, SessionInfo,
};
use agent_client_protocol::{ConnectionTo, Dispatch, HandleDispatchFrom, Handled};
use chrono::Utc;
use tokio::sync::{mpsc, oneshot};

use crate::agent::AgentFactory;
use crate::error::Error;
use crate::session::{LifecycleEvent, LifecycleEventSender};
use crate::state::{DaemonState, SessionRecord};

// ---------------------------------------------------------------------------
// DaemonMessage — inputs to the actor
// ---------------------------------------------------------------------------

// ANCHOR: daemon-message
#[allow(dead_code)]
pub(super) enum DaemonMessage {
    /// Client sent `initialize` — resolve capabilities from cache or probe a temp agent.
    /// Reply carries the agent's advertised capabilities.
    Initialize {
        req: InitializeRequest,
        reply: oneshot::Sender<Result<InitializeResponse, Error>>,
    },
    /// Client sent `session/list` — return known sessions, optionally filtered by cwd.
    ListSessions {
        req: ListSessionsRequest,
        reply: oneshot::Sender<ListSessionsResponse>,
    },
    /// Client task asks whether the agent for a session is alive or dead so it can
    /// decide whether to spawn a new agent (dead) or reuse the existing one (alive).
    QuerySessionState {
        session_id: String,
        reply: oneshot::Sender<Option<SessionLivenessInfo>>,
    },

    /// Client task completed `session/new`: agent is spawned, initialized, and bridged.
    /// Actor persists the session record and stores connection handles.
    SessionCreated {
        session_id: String,
        cwd: PathBuf,
        client_cx: ConnectionTo<agent_client_protocol::Client>,
        agent_cx: ConnectionTo<agent_client_protocol::Agent>,
    },
    /// Client task completed `session/load` or `session/resume`: actor updates the
    /// stored client_cx (and optionally agent_cx if a new agent was spawned).
    /// When `replay_to_client` is true the actor replays buffered notifications.
    SessionReconnected {
        session_id: String,
        client_cx: ConnectionTo<agent_client_protocol::Client>,
        agent_cx: Option<ConnectionTo<agent_client_protocol::Agent>>,
        replay_to_client: bool,
    },

    /// A dispatch arrived from a client (post-session-establishment).
    /// Actor forwards it to the session's agent via `send_proxied_message`.
    ClientMessage {
        session_id: String,
        dispatch: Dispatch,
    },
    /// A dispatch arrived from an agent (response or notification).
    /// Actor buffers notifications, updates lifecycle, and forwards to the client.
    AgentMessage {
        session_id: String,
        dispatch: Dispatch,
    },

    /// The client's ACP connection closed. Actor drops client_cx and starts
    /// the quiescence timer (generation-guarded).
    ClientDisconnected { session_id: String },
    /// The agent process exited unexpectedly. Actor marks the session dead.
    AgentExited { session_id: String },
    /// Quiescence window elapsed without activity. If generation still matches,
    /// actor transitions to Quiescent and starts the idle timer.
    AgentQuiescent { session_id: String, generation: u64 },
    /// Idle timeout elapsed. If generation still matches, actor kills the agent
    /// (drops agent_cx, clears buffer).
    IdleTimeoutElapsed { session_id: String, generation: u64 },
    /// Periodic sweep: remove sessions whose cwd no longer exists on disk.
    CwdHealthCheck,
}
// ANCHOR_END: daemon-message

/// Info returned by QuerySessionState so the client task can decide
/// whether to spawn a new agent or reuse the existing one.
pub(super) struct SessionLivenessInfo {
    pub(super) agent_dead: bool,
    pub(super) cwd: PathBuf,
}

// ---------------------------------------------------------------------------
// LifecycleState
// ---------------------------------------------------------------------------

/// Per-session agent lifecycle. Transitions are driven by DaemonMessage processing.
///
/// ```text
/// AgentDead → Spawning → Active → TurnComplete → Quiescent → IdleTimerRunning → AgentDead
///                          ↑ (on_message / on_client_connect resets to Active)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
enum LifecycleState {
    /// No agent process running. Next: spawn on demand.
    AgentDead,
    /// Agent process launched, awaiting initialize response.
    Spawning,
    /// Agent initialized and exchanging messages with a client.
    Active,
    /// Agent finished a turn (prompt/end received), no new activity yet.
    TurnComplete,
    /// Pipe silent for the quiescence window; idle timer started.
    Quiescent,
    /// Idle timer running; if it fires without interruption, agent is killed.
    IdleTimerRunning,
}

#[allow(dead_code)]
impl LifecycleState {
    fn on_message(self) -> Self {
        match self {
            Self::AgentDead => self,
            _ => Self::Active,
        }
    }

    fn on_turn_complete(self) -> Option<Self> {
        match self {
            Self::Active => Some(Self::TurnComplete),
            _ => None,
        }
    }

    fn on_quiescence(self) -> Option<Self> {
        match self {
            Self::TurnComplete => Some(Self::Quiescent),
            _ => None,
        }
    }

    fn on_idle_timer_start(self) -> Option<Self> {
        match self {
            Self::Quiescent => Some(Self::IdleTimerRunning),
            _ => None,
        }
    }

    fn on_spawn_request(self) -> Option<Self> {
        match self {
            Self::AgentDead => Some(Self::Spawning),
            _ => None,
        }
    }

    fn on_spawn_complete(self) -> Option<Self> {
        match self {
            Self::Spawning => Some(Self::Active),
            _ => None,
        }
    }

    fn on_kill(self) -> Self {
        Self::AgentDead
    }

    fn on_client_connect(self) -> Self {
        match self {
            Self::AgentDead => self,
            _ => Self::Active,
        }
    }
}

// ---------------------------------------------------------------------------
// LiveSession
// ---------------------------------------------------------------------------

/// In-memory state for one session, owned exclusively by the actor.
/// Combines the persistent record with runtime handles and lifecycle tracking.
struct LiveSession {
    record: SessionRecord,
    lifecycle_state: LifecycleState,
    /// Handle to the currently-connected client (None when disconnected).
    client_cx: Option<ConnectionTo<agent_client_protocol::Client>>,
    /// Handle to the agent process (None when dead/killed).
    agent_cx: Option<ConnectionTo<agent_client_protocol::Agent>>,
    /// Buffered agent notifications for session/load replay.
    buffer: Vec<serde_json::Value>,
    /// Monotonic counter bumped on any state change; timer messages carry the
    /// generation they were spawned at — stale timers are discarded on mismatch.
    generation: u64,
    /// Guards against infinite respawn loops on repeated agent crashes.
    respawn_attempted: bool,
}

impl LiveSession {
    fn new(record: SessionRecord) -> Self {
        Self {
            record,
            lifecycle_state: LifecycleState::AgentDead,
            client_cx: None,
            agent_cx: None,
            buffer: Vec::new(),
            generation: 0,
            respawn_attempted: false,
        }
    }

    fn kill_agent(&mut self) {
        self.agent_cx = None;
        self.lifecycle_state = LifecycleState::AgentDead;
    }
}

// ---------------------------------------------------------------------------
// DaemonActor
// ---------------------------------------------------------------------------

/// Central actor: owns all session state, processes DaemonMessages sequentially.
/// Spawned as a single `tokio::spawn` task — no internal concurrency, no mutexes.
pub(super) struct DaemonActor {
    sessions: HashMap<String, LiveSession>,
    /// Persistent state (session records + capabilities cache), saved to disk on mutation.
    state: DaemonState,
    state_path: PathBuf,
    /// Used only for the `Initialize` cold-cache path (temp agent probe).
    factory: Arc<dyn AgentFactory>,
    idle_timeout: std::time::Duration,
    quiescence_timeout: std::time::Duration,
    /// Observer channel for lifecycle events (tests, tracing).
    event_tx: Option<LifecycleEventSender>,
    /// Clone given to spawned timer tasks so they can send back to this actor.
    actor_tx: mpsc::UnboundedSender<DaemonMessage>,
}

impl DaemonActor {
    pub(super) fn new(
        state: DaemonState,
        state_path: PathBuf,
        factory: Arc<dyn AgentFactory>,
        idle_timeout: std::time::Duration,
        quiescence_timeout: std::time::Duration,
        event_tx: Option<LifecycleEventSender>,
        actor_tx: mpsc::UnboundedSender<DaemonMessage>,
    ) -> Self {
        let mut actor = Self {
            sessions: HashMap::new(),
            state: state.clone(),
            state_path,
            factory,
            idle_timeout,
            quiescence_timeout,
            event_tx,
            actor_tx,
        };
        actor.rehydrate_from_state(&state);
        actor
    }

    fn rehydrate_from_state(&mut self, state: &DaemonState) {
        for record in &state.sessions {
            if !self.sessions.contains_key(&record.session_id) {
                self.sessions
                    .insert(record.session_id.clone(), LiveSession::new(record.clone()));
            }
        }
    }

    pub(super) async fn run(&mut self, mut rx: mpsc::UnboundedReceiver<DaemonMessage>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                DaemonMessage::Initialize { req, reply } => {
                    let result = self.handle_initialize(req).await;
                    let _ = reply.send(result);
                }
                DaemonMessage::ListSessions { req, reply } => {
                    let response = self.handle_list_sessions(req);
                    let _ = reply.send(response);
                }
                DaemonMessage::QuerySessionState { session_id, reply } => {
                    let _ = reply.send(self.query_session_state(&session_id));
                }
                DaemonMessage::SessionCreated {
                    session_id,
                    cwd,
                    client_cx,
                    agent_cx,
                } => {
                    self.handle_session_created(session_id, cwd, client_cx, agent_cx);
                }
                DaemonMessage::SessionReconnected {
                    session_id,
                    client_cx,
                    agent_cx,
                    replay_to_client,
                } => {
                    self.handle_session_reconnected(
                        &session_id,
                        client_cx,
                        agent_cx,
                        replay_to_client,
                    );
                }
                DaemonMessage::ClientMessage {
                    session_id,
                    dispatch,
                } => {
                    self.route_client_to_agent(&session_id, dispatch);
                }
                DaemonMessage::AgentMessage {
                    session_id,
                    dispatch,
                } => {
                    self.route_agent_to_client(&session_id, dispatch);
                }
                DaemonMessage::ClientDisconnected { session_id } => {
                    self.handle_client_disconnected(&session_id);
                }
                DaemonMessage::AgentExited { session_id } => {
                    self.handle_agent_exited(&session_id);
                }
                DaemonMessage::AgentQuiescent {
                    session_id,
                    generation,
                } => {
                    self.handle_agent_quiescent(&session_id, generation);
                }
                DaemonMessage::IdleTimeoutElapsed {
                    session_id,
                    generation,
                } => {
                    self.handle_idle_timeout(&session_id, generation);
                }
                DaemonMessage::CwdHealthCheck => {
                    self.handle_cwd_health_check();
                }
            }
        }
    }

    fn emit(&self, event: LifecycleEvent) {
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(event);
        }
    }

    // -----------------------------------------------------------------------
    // Initialize (the only handler that does async I/O — acceptable since
    // it's a one-time capabilities probe, not per-session)
    // -----------------------------------------------------------------------

    async fn handle_initialize(
        &mut self,
        req: InitializeRequest,
    ) -> Result<InitializeResponse, Error> {
        let caps_value =
            serde_json::to_value(&req.client_capabilities).unwrap_or(serde_json::Value::Null);

        if let Some(cached) = &self.state.capabilities_cache
            && cached.matches(&caps_value)
        {
            let response: InitializeResponse = serde_json::from_value(cached.response.clone())
                .map_err(|e| Error::AgentSpawn(format!("corrupt capabilities cache: {e}")))?;
            return Ok(response);
        }

        // Cold cache: spawn a temp agent. This blocks the actor loop briefly
        // but only happens once per daemon lifetime (or on capability change).
        let response =
            crate::agent::AgentManager::get_capabilities(&req, self.factory.as_ref()).await?;

        let response_value = serde_json::to_value(&response).unwrap_or(serde_json::Value::Null);
        self.state.capabilities_cache = Some(crate::state::CachedCapabilities {
            client_capabilities_hash: crate::state::CachedCapabilities::hash_capabilities(
                &caps_value,
            ),
            response: response_value,
        });
        let _ = self.state.save(&self.state_path);

        Ok(response)
    }

    // -----------------------------------------------------------------------
    // ListSessions
    // -----------------------------------------------------------------------

    fn handle_list_sessions(&self, req: ListSessionsRequest) -> ListSessionsResponse {
        let sessions = self.state.list_sessions_by_cwd(req.cwd.as_deref());
        let session_infos: Vec<SessionInfo> = sessions
            .into_iter()
            .map(|s| {
                SessionInfo::new(s.session_id.clone(), s.cwd.clone())
                    .updated_at(s.updated_at.to_rfc3339())
            })
            .collect();
        ListSessionsResponse::new(session_infos)
    }

    // -----------------------------------------------------------------------
    // QuerySessionState — lets client task decide spawn vs reuse
    // -----------------------------------------------------------------------

    fn query_session_state(&self, session_id: &str) -> Option<SessionLivenessInfo> {
        self.state.find_session(session_id)?;
        let session = self.sessions.get(session_id)?;
        Some(SessionLivenessInfo {
            agent_dead: session.lifecycle_state == LifecycleState::AgentDead,
            cwd: session.record.cwd.clone(),
        })
    }

    // -----------------------------------------------------------------------
    // SessionCreated — client task spawned the agent, actor records state
    // -----------------------------------------------------------------------

    fn handle_session_created(
        &mut self,
        session_id: String,
        cwd: PathBuf,
        client_cx: ConnectionTo<agent_client_protocol::Client>,
        agent_cx: ConnectionTo<agent_client_protocol::Agent>,
    ) {
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
            LiveSession {
                record,
                lifecycle_state: LifecycleState::Active,
                client_cx: Some(client_cx),
                agent_cx: Some(agent_cx),
                buffer: Vec::new(),
                generation: 0,
                respawn_attempted: false,
            },
        );

        self.emit(LifecycleEvent::SessionCreated { session_id });
    }

    // -----------------------------------------------------------------------
    // SessionReconnected — client task loaded/resumed, actor rewires state
    // -----------------------------------------------------------------------

    fn handle_session_reconnected(
        &mut self,
        session_id: &str,
        client_cx: ConnectionTo<agent_client_protocol::Client>,
        agent_cx: Option<ConnectionTo<agent_client_protocol::Agent>>,
        replay_to_client: bool,
    ) {
        let Some(session) = self.sessions.get_mut(session_id) else {
            return;
        };

        if replay_to_client {
            for msg in &session.buffer {
                if let Ok(notif) = serde_json::from_value::<
                    agent_client_protocol::schema::SessionNotification,
                >(msg.clone())
                {
                    let _ = client_cx.send_notification(notif);
                }
            }
        }

        if let Some(new_agent_cx) = agent_cx {
            session.agent_cx = Some(new_agent_cx);
            session.buffer = Vec::new();
            session.respawn_attempted = false;
        }

        session.client_cx = Some(client_cx);
        session.lifecycle_state = LifecycleState::Active;
        session.generation += 1;
    }

    // -----------------------------------------------------------------------
    // Message routing
    // -----------------------------------------------------------------------

    // ANCHOR: route-messages
    fn route_client_to_agent(&self, session_id: &str, dispatch: Dispatch) {
        let Some(session) = self.sessions.get(session_id) else {
            tracing::warn!(session_id, "client message for unknown session");
            return;
        };
        let Some(agent_cx) = &session.agent_cx else {
            tracing::warn!(session_id, "client message but no agent connection");
            return;
        };
        if let Err(e) = agent_cx.send_proxied_message(dispatch) {
            tracing::error!(session_id, error = %e, "failed to forward to agent");
        }
    }

    fn route_agent_to_client(&mut self, session_id: &str, dispatch: Dispatch) {
        let Some(session) = self.sessions.get_mut(session_id) else {
            tracing::warn!(session_id, "agent message for unknown session");
            return;
        };

        if let Dispatch::Notification(ref notif) = dispatch
            && let Ok(value) = serde_json::to_value(notif)
        {
            session.buffer.push(value);
        }

        session.lifecycle_state = session.lifecycle_state.on_message();
        session.generation += 1;

        let Some(client_cx) = &session.client_cx else {
            return;
        };
        if let Err(e) = client_cx.send_proxied_message(dispatch) {
            tracing::error!(session_id, error = %e, "failed to forward to client");
        }
    }

    // ANCHOR_END: route-messages

    // -----------------------------------------------------------------------
    // Disconnect and timers
    // -----------------------------------------------------------------------

    // ANCHOR: disconnect-and-idle
    fn handle_client_disconnected(&mut self, session_id: &str) {
        let Some(session) = self.sessions.get_mut(session_id) else {
            return;
        };

        session.client_cx = None;
        session.generation += 1;

        if session.lifecycle_state == LifecycleState::AgentDead {
            return;
        }

        let current_gen = session.generation;
        let sid = session_id.to_string();
        let tx = self.actor_tx.clone();
        let quiescence_timeout = self.quiescence_timeout;

        tokio::spawn(async move {
            tokio::time::sleep(quiescence_timeout).await;
            let _ = tx.send(DaemonMessage::AgentQuiescent {
                session_id: sid,
                generation: current_gen,
            });
        });
    }

    fn handle_agent_quiescent(&mut self, session_id: &str, generation: u64) {
        let Some(session) = self.sessions.get_mut(session_id) else {
            return;
        };

        if session.generation != generation {
            return;
        }

        session.lifecycle_state = LifecycleState::Quiescent;
        let current_gen = session.generation;

        self.emit(LifecycleEvent::AgentQuiescent {
            session_id: session_id.to_string(),
        });

        let sid = session_id.to_string();
        let tx = self.actor_tx.clone();
        let idle_timeout = self.idle_timeout;

        tokio::spawn(async move {
            tokio::time::sleep(idle_timeout).await;
            let _ = tx.send(DaemonMessage::IdleTimeoutElapsed {
                session_id: sid,
                generation: current_gen,
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

        session.kill_agent();
        session.buffer.clear();
        tracing::info!(session_id, "agent killed due to idle timeout");
        self.emit(LifecycleEvent::AgentKilledIdle {
            session_id: session_id.to_string(),
        });
    }

    // ANCHOR_END: disconnect-and-idle

    // -----------------------------------------------------------------------
    // Agent exit (crash detection — respawn delegated back to client task
    // if one is connected; otherwise mark dead)
    // -----------------------------------------------------------------------

    // ANCHOR: handle-agent-exited
    fn handle_agent_exited(&mut self, session_id: &str) {
        let Some(session) = self.sessions.get_mut(session_id) else {
            return;
        };

        if session.lifecycle_state == LifecycleState::AgentDead {
            return;
        }

        session.agent_cx = None;
        session.buffer.clear();
        session.lifecycle_state = LifecycleState::AgentDead;
        tracing::warn!(session_id, "agent exited unexpectedly");
    }

    // ANCHOR_END: handle-agent-exited

    // -----------------------------------------------------------------------
    // CWD health check
    // -----------------------------------------------------------------------

    // ANCHOR: cwd-health-check
    fn handle_cwd_health_check(&mut self) {
        let to_remove: Vec<String> = self
            .sessions
            .iter()
            .filter(|(_, s)| !s.record.cwd.exists())
            .map(|(id, _)| id.clone())
            .collect();

        for sid in &to_remove {
            if let Some(mut session) = self.sessions.remove(sid) {
                session.kill_agent();
            }
            self.state.remove_session(sid);
            tracing::info!(session_id = sid.as_str(), "session removed: cwd deleted");
        }

        if !to_remove.is_empty() {
            let _ = self.state.save(&self.state_path);
        }
    }
    // ANCHOR_END: cwd-health-check
}

// ---------------------------------------------------------------------------
// Forwarder handlers
// ---------------------------------------------------------------------------

struct ClientForwarder {
    session_id: String,
    actor_tx: mpsc::UnboundedSender<DaemonMessage>,
}

impl HandleDispatchFrom<agent_client_protocol::Client> for ClientForwarder {
    async fn handle_dispatch_from(
        &mut self,
        message: Dispatch,
        _client_cx: ConnectionTo<agent_client_protocol::Client>,
    ) -> agent_client_protocol::schema::Result<Handled<Dispatch>> {
        let _ = self.actor_tx.send(DaemonMessage::ClientMessage {
            session_id: self.session_id.clone(),
            dispatch: message,
        });
        Ok(Handled::Yes)
    }

    fn describe_chain(&self) -> impl std::fmt::Debug {
        "ClientForwarder"
    }
}

struct AgentForwarder {
    session_id: String,
    actor_tx: mpsc::UnboundedSender<DaemonMessage>,
}

impl HandleDispatchFrom<agent_client_protocol::Agent> for AgentForwarder {
    async fn handle_dispatch_from(
        &mut self,
        message: Dispatch,
        _agent_cx: ConnectionTo<agent_client_protocol::Agent>,
    ) -> agent_client_protocol::schema::Result<Handled<Dispatch>> {
        let _ = self.actor_tx.send(DaemonMessage::AgentMessage {
            session_id: self.session_id.clone(),
            dispatch: message,
        });
        Ok(Handled::Yes)
    }

    fn describe_chain(&self) -> impl std::fmt::Debug {
        "AgentForwarder"
    }
}

pub(super) fn install_client_forwarder(
    client_cx: &ConnectionTo<agent_client_protocol::Client>,
    session_id: &str,
    actor_tx: mpsc::UnboundedSender<DaemonMessage>,
) -> Result<(), Error> {
    client_cx
        .add_dynamic_handler(ClientForwarder {
            session_id: session_id.to_string(),
            actor_tx,
        })
        .map_err(|e| Error::AgentSpawn(format!("failed to install client forwarder: {e}")))?
        .run_indefinitely();
    Ok(())
}

pub(super) fn install_agent_forwarder(
    agent_cx: &ConnectionTo<agent_client_protocol::Agent>,
    session_id: &str,
    actor_tx: mpsc::UnboundedSender<DaemonMessage>,
) -> Result<(), Error> {
    agent_cx
        .add_dynamic_handler(AgentForwarder {
            session_id: session_id.to_string(),
            actor_tx,
        })
        .map_err(|e| Error::AgentSpawn(format!("failed to install agent forwarder: {e}")))?
        .run_indefinitely();
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_state_defaults_to_dead() {
        let session = LiveSession::new(SessionRecord {
            session_id: "test".to_string(),
            cwd: PathBuf::from("/tmp"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        assert_eq!(session.lifecycle_state, LifecycleState::AgentDead);
    }

    #[test]
    fn lifecycle_message_resets_to_active() {
        assert_eq!(LifecycleState::Active.on_message(), LifecycleState::Active);
        assert_eq!(
            LifecycleState::TurnComplete.on_message(),
            LifecycleState::Active
        );
        assert_eq!(
            LifecycleState::Quiescent.on_message(),
            LifecycleState::Active
        );
        assert_eq!(
            LifecycleState::IdleTimerRunning.on_message(),
            LifecycleState::Active
        );
    }

    #[test]
    fn lifecycle_message_on_dead_stays_dead() {
        assert_eq!(
            LifecycleState::AgentDead.on_message(),
            LifecycleState::AgentDead
        );
    }

    #[test]
    fn lifecycle_turn_complete_from_active() {
        assert_eq!(
            LifecycleState::Active.on_turn_complete(),
            Some(LifecycleState::TurnComplete)
        );
    }

    #[test]
    fn lifecycle_turn_complete_rejected_from_other_states() {
        assert_eq!(LifecycleState::AgentDead.on_turn_complete(), None);
        assert_eq!(LifecycleState::TurnComplete.on_turn_complete(), None);
        assert_eq!(LifecycleState::Quiescent.on_turn_complete(), None);
    }

    #[test]
    fn lifecycle_quiescence_from_turn_complete() {
        assert_eq!(
            LifecycleState::TurnComplete.on_quiescence(),
            Some(LifecycleState::Quiescent)
        );
    }

    #[test]
    fn lifecycle_idle_timer_from_quiescent() {
        assert_eq!(
            LifecycleState::Quiescent.on_idle_timer_start(),
            Some(LifecycleState::IdleTimerRunning)
        );
    }

    #[test]
    fn lifecycle_spawn_from_dead() {
        assert_eq!(
            LifecycleState::AgentDead.on_spawn_request(),
            Some(LifecycleState::Spawning)
        );
        assert_eq!(LifecycleState::Active.on_spawn_request(), None);
    }

    #[test]
    fn lifecycle_spawn_complete() {
        assert_eq!(
            LifecycleState::Spawning.on_spawn_complete(),
            Some(LifecycleState::Active)
        );
    }

    #[test]
    fn lifecycle_kill_always_goes_dead() {
        assert_eq!(LifecycleState::Active.on_kill(), LifecycleState::AgentDead);
        assert_eq!(
            LifecycleState::Spawning.on_kill(),
            LifecycleState::AgentDead
        );
        assert_eq!(
            LifecycleState::IdleTimerRunning.on_kill(),
            LifecycleState::AgentDead
        );
    }

    #[test]
    fn lifecycle_client_connect_resets_to_active() {
        assert_eq!(
            LifecycleState::IdleTimerRunning.on_client_connect(),
            LifecycleState::Active
        );
        assert_eq!(
            LifecycleState::Quiescent.on_client_connect(),
            LifecycleState::Active
        );
        assert_eq!(
            LifecycleState::AgentDead.on_client_connect(),
            LifecycleState::AgentDead
        );
    }

    #[test]
    fn lifecycle_full_happy_path() {
        let mut state = LifecycleState::AgentDead;
        state = state.on_spawn_request().unwrap();
        assert_eq!(state, LifecycleState::Spawning);
        state = state.on_spawn_complete().unwrap();
        assert_eq!(state, LifecycleState::Active);
        state = state.on_turn_complete().unwrap();
        assert_eq!(state, LifecycleState::TurnComplete);
        state = state.on_quiescence().unwrap();
        assert_eq!(state, LifecycleState::Quiescent);
        state = state.on_idle_timer_start().unwrap();
        assert_eq!(state, LifecycleState::IdleTimerRunning);
        state = state.on_kill();
        assert_eq!(state, LifecycleState::AgentDead);
    }
}
