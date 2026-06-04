use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("agent spawn failed: {0}")]
    AgentSpawn(String),

    #[error("session not found: {0}")]
    SessionNotFound(String),

    #[error("invalid working directory: {}", path.display())]
    InvalidCwd { path: PathBuf },

    #[error("state file error: {0}")]
    State(#[from] StateError),

    #[error("ACP protocol error: {0}")]
    Acp(#[from] agent_client_protocol::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("failed to read state file: {0}")]
    Read(std::io::Error),

    #[error("failed to write state file: {0}")]
    Write(std::io::Error),

    #[error("failed to parse state file: {0}")]
    Parse(serde_json::Error),
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
