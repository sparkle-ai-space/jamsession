use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::StateError;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DaemonState {
    version: u32,
    pub(super) sessions: Vec<SessionRecord>,
    capabilities_cache: Option<CachedCapabilities>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SessionRecord {
    pub(super) session_id: String,
    pub(super) cwd: PathBuf,
    pub(super) created_at: DateTime<Utc>,
    pub(super) updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedCapabilities {
    client_capabilities_hash: u64,
    response: serde_json::Value,
}

impl DaemonState {
    fn new() -> Self {
        Self {
            version: 1,
            sessions: Vec::new(),
            capabilities_cache: None,
        }
    }

    pub fn state_path() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".jamsession")
            .join("state.json")
    }

    pub(super) fn load(path: &Path) -> Self {
        let contents = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => return Self::new(),
        };
        serde_json::from_str(&contents).unwrap_or_else(|e| {
            tracing::warn!("corrupt state file, starting fresh: {e}");
            Self::new()
        })
    }

    pub(super) fn save(&self, path: &Path) -> Result<(), StateError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(StateError::Write)?;
        }
        let tmp_path = path.with_extension("json.tmp");
        let contents = serde_json::to_string_pretty(self).map_err(StateError::Parse)?;
        std::fs::write(&tmp_path, contents).map_err(StateError::Write)?;
        std::fs::rename(&tmp_path, path).map_err(StateError::Write)?;
        Ok(())
    }

    pub(super) fn list_sessions_by_cwd(&self, cwd: Option<&Path>) -> Vec<&SessionRecord> {
        match cwd {
            Some(cwd) => self.sessions.iter().filter(|s| s.cwd == cwd).collect(),
            None => self.sessions.iter().collect(),
        }
    }

    pub(super) fn add_session(&mut self, record: SessionRecord) {
        self.sessions.push(record);
    }

    pub(super) fn remove_session(&mut self, session_id: &str) {
        self.sessions.retain(|s| s.session_id != session_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn round_trip_empty_state() {
        let state = DaemonState::new();
        let json = serde_json::to_string(&state).unwrap();
        let loaded: DaemonState = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.version, 1);
        assert!(loaded.sessions.is_empty());
        assert!(loaded.capabilities_cache.is_none());
    }

    #[test]
    fn round_trip_with_sessions() {
        let mut state = DaemonState::new();
        state.add_session(SessionRecord {
            session_id: "sess_123".to_string(),
            cwd: PathBuf::from("/tmp/project"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        let json = serde_json::to_string(&state).unwrap();
        let loaded: DaemonState = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.sessions.len(), 1);
        assert_eq!(loaded.sessions[0].session_id, "sess_123");
    }

    #[test]
    fn atomic_write_and_read_back() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.json");

        let mut state = DaemonState::new();
        state.add_session(SessionRecord {
            session_id: "sess_abc".to_string(),
            cwd: PathBuf::from("/home/user/code"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        state.save(&path).unwrap();

        let loaded = DaemonState::load(&path);
        assert_eq!(loaded.sessions.len(), 1);
        assert_eq!(loaded.sessions[0].session_id, "sess_abc");
    }

    #[test]
    fn load_missing_file_returns_empty() {
        let state = DaemonState::load(Path::new("/nonexistent/path/state.json"));
        assert_eq!(state.version, 1);
        assert!(state.sessions.is_empty());
    }

    #[test]
    fn load_corrupt_file_returns_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.json");
        fs::write(&path, "not valid json {{{").unwrap();

        let state = DaemonState::load(&path);
        assert_eq!(state.version, 1);
        assert!(state.sessions.is_empty());
    }
}
