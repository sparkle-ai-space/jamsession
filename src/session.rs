use std::collections::HashMap;
use std::path::Path;

use agent_client_protocol::ConnectionTo;
use agent_client_protocol::schema::{
    ContentBlock, LoadSessionRequest, LoadSessionResponse, NewSessionRequest, NewSessionResponse,
    PromptRequest, ResumeSessionResponse, TextContent,
};
use chrono::Utc;
use tokio::sync::Mutex;

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

pub struct LiveSession {
    pub record: SessionRecord,
    pub lifecycle_state: LifecycleState,
    pub client_count: usize,
    pub buffer: Vec<serde_json::Value>,
    pub agent_cx: Option<ConnectionTo<agent_client_protocol::Agent>>,
}

pub struct SessionManager {
    sessions: Mutex<HashMap<String, LiveSession>>,
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
        }
    }

    pub async fn handle_new_session(
        &self,
        req: NewSessionRequest,
        state: &Mutex<DaemonState>,
        state_path: &Path,
    ) -> Result<NewSessionResponse, Error> {
        if !req.cwd.is_absolute() || !req.cwd.exists() {
            return Err(Error::InvalidCwd { path: req.cwd });
        }

        let session_id = format!("sess_{}", uuid::Uuid::new_v4().simple());

        let record = SessionRecord {
            session_id: session_id.clone(),
            cwd: req.cwd.clone(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        // Persist to state
        {
            let mut daemon_state = state.lock().await;
            daemon_state.add_session(record.clone());
            let _ = daemon_state.save(state_path);
        }

        // Register live session (agent will be connected later via bridge)
        {
            let mut sessions = self.sessions.lock().await;
            sessions.insert(
                session_id.clone(),
                LiveSession {
                    record,
                    lifecycle_state: LifecycleState::AgentDead,
                    client_count: 1,
                    buffer: Vec::new(),
                    agent_cx: None,
                },
            );
        }

        Ok(NewSessionResponse::new(session_id))
    }

    pub async fn handle_load_session(
        &self,
        req: LoadSessionRequest,
        state: &Mutex<DaemonState>,
        _state_path: &Path,
    ) -> Result<LoadSessionResponse, Error> {
        let session_id = &req.session_id.0;

        let daemon_state = state.lock().await;
        if daemon_state.find_session(session_id).is_none() {
            return Err(Error::SessionNotFound(session_id.to_string()));
        }
        drop(daemon_state);

        // Enforce one-client-per-session: disconnect existing client
        {
            let mut sessions = self.sessions.lock().await;
            if let Some(session) = sessions.get_mut(session_id.as_ref()) {
                session.client_count = 1;
            }
        }

        Ok(LoadSessionResponse::new())
    }

    pub async fn handle_resume_session(
        &self,
        req: agent_client_protocol::schema::ResumeSessionRequest,
        state: &Mutex<DaemonState>,
    ) -> Result<ResumeSessionResponse, Error> {
        let session_id = &req.session_id.0;

        let daemon_state = state.lock().await;
        if daemon_state.find_session(session_id).is_none() {
            return Err(Error::SessionNotFound(session_id.to_string()));
        }
        drop(daemon_state);

        // Enforce one-client-per-session
        {
            let mut sessions = self.sessions.lock().await;
            if let Some(session) = sessions.get_mut(session_id.as_ref()) {
                session.client_count = 1;
            }
        }

        Ok(ResumeSessionResponse::new())
    }

    pub fn guidelines_prompt(session_id: &str) -> PromptRequest {
        PromptRequest::new(
            agent_client_protocol::schema::SessionId::new(session_id),
            vec![ContentBlock::Text(TextContent::new(GUIDELINES))],
        )
    }

    pub async fn disconnect_client(&self, session_id: &str) {
        let mut sessions = self.sessions.lock().await;
        if let Some(session) = sessions.get_mut(session_id) {
            session.client_count = session.client_count.saturating_sub(1);
        }
    }

    pub async fn kill_all_agents(&self) {
        let sessions = self.sessions.lock().await;
        for (_id, session) in sessions.iter() {
            if session.lifecycle_state != LifecycleState::AgentDead {
                // Agent kill handled via dropping the connection
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_state_defaults_to_dead() {
        let session = LiveSession {
            record: SessionRecord {
                session_id: "test".to_string(),
                cwd: std::path::PathBuf::from("/tmp"),
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            lifecycle_state: LifecycleState::AgentDead,
            client_count: 0,
            buffer: Vec::new(),
            agent_cx: None,
        };
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
        // Dead stays dead (can't connect to a dead agent)
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
