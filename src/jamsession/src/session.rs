use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use agent_client_protocol::schema::{
    ContentBlock, LoadSessionRequest, LoadSessionResponse, NewSessionRequest, NewSessionResponse,
    PromptRequest, ResumeSessionResponse, SessionId, TextContent,
};
use agent_client_protocol::{ConnectionTo, Dispatch, HandleDispatchFrom, Handled};
use chrono::Utc;
use tokio::sync::Notify;

use crate::agent::{AgentManager, AgentTransport};
use crate::bridge::{ActivitySignal, BridgeHandler, MessageBuffer, ReverseBridgeHandler};
use crate::error::Error;
use crate::state::{DaemonState, SessionRecord};

static GUIDELINES: &str = include_str!("guidelines.md");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleState {
    AgentDead,
    Spawning,
    Active,
    TurnComplete,
    Quiescent,
    IdleTimerRunning,
}

impl LifecycleState {
    pub fn on_message(self) -> Self {
        match self {
            Self::AgentDead => self,
            _ => Self::Active,
        }
    }

    pub fn on_turn_complete(self) -> Option<Self> {
        match self {
            Self::Active => Some(Self::TurnComplete),
            _ => None,
        }
    }

    pub fn on_quiescence(self) -> Option<Self> {
        match self {
            Self::TurnComplete => Some(Self::Quiescent),
            _ => None,
        }
    }

    pub fn on_idle_timer_start(self) -> Option<Self> {
        match self {
            Self::Quiescent => Some(Self::IdleTimerRunning),
            _ => None,
        }
    }

    pub fn on_spawn_request(self) -> Option<Self> {
        match self {
            Self::AgentDead => Some(Self::Spawning),
            _ => None,
        }
    }

    pub fn on_spawn_complete(self) -> Option<Self> {
        match self {
            Self::Spawning => Some(Self::Active),
            _ => None,
        }
    }

    pub fn on_kill(self) -> Self {
        Self::AgentDead
    }

    pub fn on_client_connect(self) -> Self {
        match self {
            Self::AgentDead => self,
            _ => Self::Active,
        }
    }
}

/// Handle used to signal a client connection to disconnect (T039).
/// When `notify_waiters()` is called, the client task should exit.
pub type ClientCancelHandle = Arc<Notify>;

pub struct LiveSession {
    pub record: SessionRecord,
    pub lifecycle_state: LifecycleState,
    pub client_count: usize,
    pub buffer: MessageBuffer,
    pub agent_cx: Option<ConnectionTo<agent_client_protocol::Agent>>,
    pub activity: ActivitySignal,
    pub client_cancel: Option<ClientCancelHandle>,
    idle_timer_handle: Option<tokio::task::JoinHandle<()>>,
    quiescence_handle: Option<tokio::task::JoinHandle<()>>,
    respawn_attempted: bool,
}

impl LiveSession {
    fn new(record: SessionRecord) -> Self {
        Self {
            record,
            lifecycle_state: LifecycleState::AgentDead,
            client_count: 0,
            buffer: Arc::new(Mutex::new(Vec::new())),
            agent_cx: None,
            activity: Arc::new(Notify::new()),
            client_cancel: None,
            idle_timer_handle: None,
            quiescence_handle: None,
            respawn_attempted: false,
        }
    }

    fn cancel_timers(&mut self) {
        if let Some(h) = self.idle_timer_handle.take() {
            h.abort();
        }
        if let Some(h) = self.quiescence_handle.take() {
            h.abort();
        }
    }
}

pub struct SessionManager {
    sessions: Arc<Mutex<HashMap<String, LiveSession>>>,
    agent_transport: AgentTransport,
    idle_timeout: std::time::Duration,
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            agent_transport: AgentTransport::default(),
            idle_timeout: std::time::Duration::from_secs(900),
        }
    }

    pub fn with_agent_transport(mut self, transport: AgentTransport) -> Self {
        self.agent_transport = transport;
        self
    }

    pub fn with_idle_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Populate in-memory sessions from persistent state (called on startup).
    /// Sessions start with AgentDead — agents are spawned on demand.
    pub fn rehydrate_from_state(&self, state: &DaemonState) {
        let mut sessions = self.sessions.lock().unwrap();
        for record in &state.sessions {
            if !sessions.contains_key(&record.session_id) {
                sessions.insert(record.session_id.clone(), LiveSession::new(record.clone()));
            }
        }
    }

    // ANCHOR: install-bridge
    /// Install bidirectional bridge between client and agent connections.
    fn install_bridge(
        client_cx: &ConnectionTo<agent_client_protocol::Client>,
        agent_cx: &ConnectionTo<agent_client_protocol::Agent>,
        buffer: &MessageBuffer,
        activity: &ActivitySignal,
    ) -> Result<(), Error> {
        client_cx
            .add_dynamic_handler(BridgeHandler::new(agent_cx.clone(), activity.clone()))
            .map_err(|e| Error::AgentSpawn(format!("failed to install bridge: {e}")))?
            .run_indefinitely();
        agent_cx
            .add_dynamic_handler(ReverseBridgeHandler::new(
                client_cx.clone(),
                buffer.clone(),
                activity.clone(),
            ))
            .map_err(|e| Error::AgentSpawn(format!("failed to install reverse bridge: {e}")))?
            .run_indefinitely();
        Ok(())
    }
    // ANCHOR_END: install-bridge

    // ANCHOR: handle-new-session
    /// Handle session/new: create session record, spawn agent, install bridge.
    /// Uses agent-returned session ID as canonical (T041).
    pub async fn handle_new_session(
        &self,
        req: NewSessionRequest,
        state: &Mutex<DaemonState>,
        state_path: &Path,
        client_cx: &ConnectionTo<agent_client_protocol::Client>,
    ) -> Result<NewSessionResponse, Error> {
        if !req.cwd.is_absolute() || !req.cwd.exists() {
            return Err(Error::InvalidCwd { path: req.cwd });
        }

        // Spawn the agent
        let agent_cx = AgentManager::spawn_agent_connection(client_cx, &self.agent_transport)?;
        AgentManager::initialize_agent(&agent_cx).await?;
        let agent_response =
            AgentManager::new_session_on_agent(&agent_cx, &req.cwd, req.mcp_servers).await?;

        // T041: use agent-returned session ID as canonical
        let session_id = agent_response.session_id.0.to_string();

        let record = SessionRecord {
            session_id: session_id.clone(),
            cwd: req.cwd.clone(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        // Persist to state
        {
            let mut daemon_state = state.lock().unwrap();
            daemon_state.add_session(record.clone());
            let _ = daemon_state.save(state_path);
        }

        // Send guidelines as first prompt
        let guidelines_prompt = PromptRequest::new(
            agent_response.session_id.clone(),
            vec![ContentBlock::Text(TextContent::new(GUIDELINES))],
        );
        agent_cx
            .send_request(guidelines_prompt)
            .block_task()
            .await
            .map_err(|e| Error::AgentSpawn(format!("guidelines delivery failed: {e}")))?;

        // Create buffer and install bidirectional bridge
        let activity: ActivitySignal = Arc::new(Notify::new());
        let buffer: MessageBuffer = Arc::new(Mutex::new(Vec::new()));
        Self::install_bridge(client_cx, &agent_cx, &buffer, &activity)?;

        // Register live session
        {
            let mut sessions = self.sessions.lock().unwrap();
            let mut live = LiveSession::new(record);
            live.lifecycle_state = LifecycleState::Active;
            live.client_count = 1;
            live.agent_cx = Some(agent_cx);
            live.buffer = buffer;
            live.activity = activity;
            live.respawn_attempted = false;
            sessions.insert(session_id.clone(), live);
        }

        Ok(NewSessionResponse::new(session_id))
    }
    // ANCHOR_END: handle-new-session

    // ANCHOR: handle-load-session
    /// Handle session/load: reconnect to existing session with history replay.
    pub async fn handle_load_session(
        &self,
        req: LoadSessionRequest,
        state: &Mutex<DaemonState>,
        _state_path: &Path,
        client_cx: &ConnectionTo<agent_client_protocol::Client>,
    ) -> Result<LoadSessionResponse, Error> {
        let session_id = &req.session_id.0;

        // Verify session exists in persistent state
        {
            let daemon_state = state.lock().unwrap();
            if daemon_state.find_session(session_id).is_none() {
                return Err(Error::SessionNotFound(session_id.to_string()));
            }
        }

        // Determine agent state under the lock, extract what we need, release the lock
        enum LoadAction {
            SpawnAgent {
                cwd: std::path::PathBuf,
                sid: String,
            },
            ReplayFromLive {
                buffer: MessageBuffer,
                activity: ActivitySignal,
                agent_cx: ConnectionTo<agent_client_protocol::Agent>,
            },
        }

        let action = {
            let mut sessions = self.sessions.lock().unwrap();
            let session = sessions
                .get_mut(session_id.as_ref())
                .ok_or_else(|| Error::SessionNotFound(session_id.to_string()))?;

            if let Some(cancel) = session.client_cancel.take() {
                cancel.notify_waiters();
            }
            session.client_count = 1;
            session.cancel_timers();

            if session.lifecycle_state == LifecycleState::AgentDead {
                session.lifecycle_state = LifecycleState::Spawning;
                LoadAction::SpawnAgent {
                    cwd: session.record.cwd.clone(),
                    sid: session_id.to_string(),
                }
            } else {
                let buffer = session.buffer.clone();
                let activity = session.activity.clone();
                let agent_cx = session
                    .agent_cx
                    .clone()
                    .ok_or_else(|| Error::AgentSpawn("agent alive but no connection".into()))?;
                session.lifecycle_state = LifecycleState::Active;
                LoadAction::ReplayFromLive {
                    buffer,
                    activity,
                    agent_cx,
                }
            }
        };

        match action {
            LoadAction::SpawnAgent { cwd, sid } => {
                let agent_cx =
                    AgentManager::spawn_agent_connection(client_cx, &self.agent_transport)?;
                AgentManager::initialize_agent(&agent_cx).await?;

                let replay_buffer: MessageBuffer = Arc::new(Mutex::new(Vec::new()));
                let activity: ActivitySignal = Arc::new(Notify::new());

                let replay_capture = replay_buffer.clone();
                agent_cx
                    .add_dynamic_handler(ReplayCapture::new(replay_capture))
                    .map_err(|e| Error::AgentSpawn(format!("replay capture failed: {e}")))?
                    .run_indefinitely();

                AgentManager::load_session_on_agent(&agent_cx, &sid, &cwd, req.mcp_servers).await?;

                {
                    let buf = replay_buffer.lock().unwrap();
                    for msg in buf.iter() {
                        if let Ok(notif) = serde_json::from_value::<
                            agent_client_protocol::schema::SessionNotification,
                        >(msg.clone())
                        {
                            let _ = client_cx.send_notification(notif);
                        }
                    }
                }

                let buffer: MessageBuffer = Arc::new(Mutex::new(Vec::new()));
                Self::install_bridge(client_cx, &agent_cx, &buffer, &activity)?;

                let mut sessions = self.sessions.lock().unwrap();
                if let Some(session) = sessions.get_mut(sid.as_str()) {
                    session.lifecycle_state = LifecycleState::Active;
                    session.agent_cx = Some(agent_cx);
                    session.buffer = buffer;
                    session.activity = activity;
                    session.respawn_attempted = false;
                }
            }
            LoadAction::ReplayFromLive {
                buffer,
                activity,
                agent_cx,
            } => {
                {
                    let buf = buffer.lock().unwrap();
                    for msg in buf.iter() {
                        if let Ok(notif) = serde_json::from_value::<
                            agent_client_protocol::schema::SessionNotification,
                        >(msg.clone())
                        {
                            let _ = client_cx.send_notification(notif);
                        }
                    }
                }
                Self::install_bridge(client_cx, &agent_cx, &buffer, &activity)?;
            }
        }

        Ok(LoadSessionResponse::new())
    }
    // ANCHOR_END: handle-load-session

    /// Handle session/resume: reconnect without history replay to client.
    pub async fn handle_resume_session(
        &self,
        req: agent_client_protocol::schema::ResumeSessionRequest,
        state: &Mutex<DaemonState>,
        client_cx: &ConnectionTo<agent_client_protocol::Client>,
    ) -> Result<ResumeSessionResponse, Error> {
        let session_id = &req.session_id.0;

        {
            let daemon_state = state.lock().unwrap();
            if daemon_state.find_session(session_id).is_none() {
                return Err(Error::SessionNotFound(session_id.to_string()));
            }
        }

        enum ResumeAction {
            SpawnAgent {
                cwd: std::path::PathBuf,
                sid: String,
            },
            BridgeLive {
                buffer: MessageBuffer,
                activity: ActivitySignal,
                agent_cx: ConnectionTo<agent_client_protocol::Agent>,
            },
        }

        let action = {
            let mut sessions = self.sessions.lock().unwrap();
            let session = sessions
                .get_mut(session_id.as_ref())
                .ok_or_else(|| Error::SessionNotFound(session_id.to_string()))?;

            if let Some(cancel) = session.client_cancel.take() {
                cancel.notify_waiters();
            }
            session.client_count = 1;
            session.cancel_timers();

            if session.lifecycle_state == LifecycleState::AgentDead {
                session.lifecycle_state = LifecycleState::Spawning;
                ResumeAction::SpawnAgent {
                    cwd: session.record.cwd.clone(),
                    sid: session_id.to_string(),
                }
            } else {
                let buffer = session.buffer.clone();
                let activity = session.activity.clone();
                let agent_cx = session
                    .agent_cx
                    .clone()
                    .ok_or_else(|| Error::AgentSpawn("agent alive but no connection".into()))?;
                session.lifecycle_state = LifecycleState::Active;
                ResumeAction::BridgeLive {
                    buffer,
                    activity,
                    agent_cx,
                }
            }
        };

        match action {
            ResumeAction::SpawnAgent { cwd, sid } => {
                let agent_cx =
                    AgentManager::spawn_agent_connection(client_cx, &self.agent_transport)?;
                AgentManager::initialize_agent(&agent_cx).await?;
                AgentManager::load_session_on_agent(&agent_cx, &sid, &cwd, req.mcp_servers).await?;

                let activity: ActivitySignal = Arc::new(Notify::new());
                let buffer: MessageBuffer = Arc::new(Mutex::new(Vec::new()));
                Self::install_bridge(client_cx, &agent_cx, &buffer, &activity)?;

                let mut sessions = self.sessions.lock().unwrap();
                if let Some(session) = sessions.get_mut(sid.as_str()) {
                    session.lifecycle_state = LifecycleState::Active;
                    session.agent_cx = Some(agent_cx);
                    session.buffer = buffer;
                    session.activity = activity;
                    session.respawn_attempted = false;
                }
            }
            ResumeAction::BridgeLive {
                buffer,
                activity,
                agent_cx,
            } => {
                Self::install_bridge(client_cx, &agent_cx, &buffer, &activity)?;
            }
        }

        Ok(ResumeSessionResponse::new())
    }

    /// Register a client cancel handle for a session (T039).
    pub fn register_client_cancel(&self, session_id: &str, cancel: ClientCancelHandle) {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(session) = sessions.get_mut(session_id) {
            session.client_cancel = Some(cancel);
        }
    }

    // ANCHOR: disconnect-client
    /// T037: Called when a client connection closes. Starts quiescence/idle countdown.
    pub fn disconnect_client(&self, session_id: &str) {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(session) = sessions.get_mut(session_id) {
            session.client_count = session.client_count.saturating_sub(1);
            session.client_cancel = None;

            if session.client_count == 0
                && session.lifecycle_state != LifecycleState::AgentDead
                && session.agent_cx.is_some()
            {
                // T036: Start quiescence timer that resets on bridge activity
                session.lifecycle_state = LifecycleState::TurnComplete;
                let sessions_ref = self.sessions.clone();
                let sid = session_id.to_string();
                let activity = session.activity.clone();
                let idle_timeout = self.idle_timeout;

                let handle = tokio::spawn(async move {
                    // T036: Wait for 10s of pipe silence (reset on activity)
                    loop {
                        let timeout = tokio::time::sleep(std::time::Duration::from_secs(10));
                        tokio::select! {
                            () = activity.notified() => {
                                continue;
                            }
                            () = timeout => {
                                break;
                            }
                        }
                    }

                    {
                        let mut guard = sessions_ref.lock().unwrap();
                        if let Some(s) = guard.get_mut(sid.as_str()) {
                            if s.client_count > 0 {
                                return;
                            }
                            s.lifecycle_state = LifecycleState::Quiescent;
                        } else {
                            return;
                        }
                    }

                    tokio::time::sleep(idle_timeout).await;

                    let mut guard = sessions_ref.lock().unwrap();
                    if let Some(s) = guard.get_mut(sid.as_str())
                        && s.lifecycle_state == LifecycleState::Quiescent
                        && s.client_count == 0
                    {
                        s.lifecycle_state = LifecycleState::AgentDead;
                        s.agent_cx = None;
                        s.buffer = Arc::new(Mutex::new(Vec::new()));
                        tracing::info!(session_id = sid, "agent killed due to idle timeout");
                    }
                });
                session.quiescence_handle = Some(handle);
            }
        }
    }
    // ANCHOR_END: disconnect-client

    // ANCHOR: handle-agent-crash
    /// T038: Handle unexpected agent death. Respawns once, notifies client.
    pub async fn handle_agent_crash(
        &self,
        session_id: &str,
        client_cx: Option<&ConnectionTo<agent_client_protocol::Client>>,
    ) {
        let (should_respawn, cwd, sid) = {
            let mut sessions = self.sessions.lock().unwrap();
            let session = match sessions.get_mut(session_id) {
                Some(s) => s,
                None => return,
            };

            session.cancel_timers();
            session.agent_cx = None;
            session.buffer = Arc::new(Mutex::new(Vec::new()));

            if session.respawn_attempted {
                session.lifecycle_state = LifecycleState::AgentDead;
                tracing::error!(session_id, "agent crashed again after respawn, giving up");
                return;
            }

            session.respawn_attempted = true;
            session.lifecycle_state = LifecycleState::Spawning;
            (true, session.record.cwd.clone(), session_id.to_string())
        };

        if !should_respawn {
            return;
        }

        tracing::warn!(session_id, "agent crashed, attempting respawn");

        // Attempt respawn
        let Some(client_cx) = client_cx else {
            let mut sessions = self.sessions.lock().unwrap();
            if let Some(s) = sessions.get_mut(sid.as_str()) {
                s.lifecycle_state = LifecycleState::AgentDead;
            }
            return;
        };

        let result: Result<(), Error> = async {
            let agent_cx = AgentManager::spawn_agent_connection(client_cx, &self.agent_transport)?;
            AgentManager::initialize_agent(&agent_cx).await?;
            AgentManager::load_session_on_agent(&agent_cx, &sid, &cwd, vec![]).await?;

            let activity: ActivitySignal = Arc::new(Notify::new());
            let buffer: MessageBuffer = Arc::new(Mutex::new(Vec::new()));
            Self::install_bridge(client_cx, &agent_cx, &buffer, &activity)?;

            let mut sessions = self.sessions.lock().unwrap();
            if let Some(session) = sessions.get_mut(sid.as_str()) {
                session.lifecycle_state = LifecycleState::Active;
                session.agent_cx = Some(agent_cx);
                session.buffer = buffer;
                session.activity = activity;
            }
            Ok(())
        }
        .await;

        if let Err(e) = result {
            tracing::error!(session_id = sid, error = %e, "respawn failed");
            let mut sessions = self.sessions.lock().unwrap();
            if let Some(s) = sessions.get_mut(sid.as_str()) {
                s.lifecycle_state = LifecycleState::AgentDead;
            }
        }
    }
    // ANCHOR_END: handle-agent-crash

    pub fn guidelines_prompt(session_id: &str) -> PromptRequest {
        PromptRequest::new(
            SessionId::new(session_id),
            vec![ContentBlock::Text(TextContent::new(GUIDELINES))],
        )
    }

    pub fn kill_all_agents(&self) {
        let mut sessions = self.sessions.lock().unwrap();
        for (_id, session) in sessions.iter_mut() {
            session.cancel_timers();
            session.agent_cx = None;
            session.lifecycle_state = LifecycleState::AgentDead;
            session.buffer = Arc::new(Mutex::new(Vec::new()));
        }
    }

    // ANCHOR: check-cwd-health
    /// Check all sessions for deleted working directories and clean up.
    pub fn check_cwd_health(&self, state: &Mutex<DaemonState>, state_path: &Path) {
        let mut sessions = self.sessions.lock().unwrap();
        let to_remove: Vec<String> = sessions
            .iter()
            .filter(|(_, s)| !s.record.cwd.exists())
            .map(|(id, _)| id.clone())
            .collect();

        for sid in &to_remove {
            if let Some(mut session) = sessions.remove(sid) {
                session.cancel_timers();
                session.agent_cx = None;
            }
        }
        drop(sessions);

        if !to_remove.is_empty() {
            let mut daemon_state = state.lock().unwrap();
            for sid in &to_remove {
                daemon_state.remove_session(sid);
                tracing::info!(session_id = sid, "session removed: cwd deleted");
            }
            let _ = daemon_state.save(state_path);
        }
    }
    // ANCHOR_END: check-cwd-health
}

/// Temporary handler to capture agent notifications during session/load replay (T040).
struct ReplayCapture {
    buffer: MessageBuffer,
}

impl ReplayCapture {
    fn new(buffer: MessageBuffer) -> Self {
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
        // Pass through (don't consume)
        Ok(Handled::No {
            message,
            retry: false,
        })
    }

    fn describe_chain(&self) -> impl std::fmt::Debug {
        "ReplayCapture"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_state_defaults_to_dead() {
        let session = LiveSession::new(SessionRecord {
            session_id: "test".to_string(),
            cwd: std::path::PathBuf::from("/tmp"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        assert_eq!(session.lifecycle_state, LifecycleState::AgentDead);
    }

    #[test]
    fn guidelines_prompt_contains_content() {
        let prompt = SessionManager::guidelines_prompt("sess_test123");
        assert!(!prompt.prompt.is_empty());
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
