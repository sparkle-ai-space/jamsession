// ANCHOR: lifecycle-event
/// Events emitted by the daemon for observability and test synchronization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleEvent {
    Initialized,
    ClientConnected,
    ClientDisconnected { session_id: Option<String> },
    SessionCreated { session_id: String },
    SessionLoaded { session_id: String },
    SessionResumed { session_id: String },
    AgentQuiescent { session_id: String },
    AgentKilledIdle { session_id: String },
}
// ANCHOR_END: lifecycle-event

impl std::fmt::Display for LifecycleEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initialized => write!(f, "Initialized"),
            Self::ClientConnected => write!(f, "ClientConnected"),
            Self::ClientDisconnected { session_id } => match session_id {
                Some(sid) => write!(f, "ClientDisconnected({sid})"),
                None => write!(f, "ClientDisconnected"),
            },
            Self::SessionCreated { session_id } => write!(f, "SessionCreated({session_id})"),
            Self::SessionLoaded { session_id } => write!(f, "SessionLoaded({session_id})"),
            Self::SessionResumed { session_id } => write!(f, "SessionResumed({session_id})"),
            Self::AgentQuiescent { session_id } => write!(f, "AgentQuiescent({session_id})"),
            Self::AgentKilledIdle { session_id } => write!(f, "AgentKilledIdle({session_id})"),
        }
    }
}

pub type LifecycleEventSender = tokio::sync::mpsc::UnboundedSender<LifecycleEvent>;
