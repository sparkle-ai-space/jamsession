use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::StateError;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DaemonState {
    pub version: u32,
    pub sessions: Vec<SessionRecord>,
    pub capabilities_cache: Option<CachedCapabilities>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecord {
    pub session_id: String,
    pub cwd: PathBuf,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedCapabilities {
    pub client_capabilities_hash: u64,
    pub response: serde_json::Value,
}

impl DaemonState {
    pub fn new() -> Self {
        Self {
            version: 1,
            sessions: Vec::new(),
            capabilities_cache: None,
        }
    }

    pub fn state_path() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".academy")
            .join("state.json")
    }

    pub fn load(path: &Path) -> Self {
        let contents = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => return Self::new(),
        };
        serde_json::from_str(&contents).unwrap_or_else(|e| {
            tracing::warn!("corrupt state file, starting fresh: {e}");
            Self::new()
        })
    }

    pub fn save(&self, path: &Path) -> Result<(), StateError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(StateError::Write)?;
        }
        let tmp_path = path.with_extension("json.tmp");
        let contents = serde_json::to_string_pretty(self).map_err(StateError::Parse)?;
        std::fs::write(&tmp_path, contents).map_err(StateError::Write)?;
        std::fs::rename(&tmp_path, path).map_err(StateError::Write)?;
        Ok(())
    }

    pub fn find_session(&self, session_id: &str) -> Option<&SessionRecord> {
        self.sessions.iter().find(|s| s.session_id == session_id)
    }

    pub fn list_sessions_by_cwd(&self, cwd: Option<&Path>) -> Vec<&SessionRecord> {
        match cwd {
            Some(cwd) => self.sessions.iter().filter(|s| s.cwd == cwd).collect(),
            None => self.sessions.iter().collect(),
        }
    }

    pub fn add_session(&mut self, record: SessionRecord) {
        self.sessions.push(record);
    }

    pub fn remove_session(&mut self, session_id: &str) {
        self.sessions.retain(|s| s.session_id != session_id);
    }
}

impl CachedCapabilities {
    pub fn hash_capabilities(capabilities: &serde_json::Value) -> u64 {
        let mut hasher = DefaultHasher::new();
        let canonical = serde_json::to_string(capabilities).unwrap_or_default();
        canonical.hash(&mut hasher);
        hasher.finish()
    }

    pub fn matches(&self, capabilities: &serde_json::Value) -> bool {
        Self::hash_capabilities(capabilities) == self.client_capabilities_hash
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

    #[test]
    fn capabilities_cache_hit_and_miss() {
        let caps_a = serde_json::json!({"fs": {"readTextFile": true}});
        let caps_b = serde_json::json!({"fs": {"readTextFile": false}});

        let cached = CachedCapabilities {
            client_capabilities_hash: CachedCapabilities::hash_capabilities(&caps_a),
            response: serde_json::json!({"protocolVersion": 1}),
        };

        assert!(cached.matches(&caps_a));
        assert!(!cached.matches(&caps_b));
    }
}
