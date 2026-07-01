use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("scope closed")]
    ScopeClosed,

    #[error("agent spawn failed: {0}")]
    AgentSpawn(String),

    #[error("session not found: {0}")]
    SessionNotFound(String),

    #[error("invalid working directory: {}", path.display())]
    InvalidCwd { path: PathBuf },

    #[error("ACP protocol error: {0}")]
    Acp(#[from] agent_client_protocol::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("database error: {0}")]
    Db(#[from] toasty::Error),

    #[error("timestamp parse error: {0}")]
    Timestamp(#[from] chrono::ParseError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("trace parse error: {0}")]
    TraceParse(#[from] crate::db::TraceParseError),
}

impl scope_tasks::SpawnError for Error {
    fn scope_closed() -> Self {
        Self::ScopeClosed
    }
}

impl From<&Error> for agent_client_protocol::Error {
    fn from(err: &Error) -> Self {
        match err {
            Error::SessionNotFound(_) => {
                agent_client_protocol::Error::invalid_params().data(err.to_string())
            }
            Error::InvalidCwd { .. } => {
                agent_client_protocol::Error::invalid_params().data(err.to_string())
            }
            Error::AgentSpawn(_) => {
                agent_client_protocol::Error::internal_error().data(err.to_string())
            }
            _ => agent_client_protocol::Error::internal_error().data(err.to_string()),
        }
    }
}
