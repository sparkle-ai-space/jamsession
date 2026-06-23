use std::path::PathBuf;
use std::sync::Arc;

use tokio::net::UnixListener;

use scope_tasks::scope;

use crate::agent::AgentFactory;
use crate::dispatcher::{self, Dispatcher, DispatcherMessage};
use crate::error::Error;
use crate::session::{LifecycleEvent, LifecycleEventSender};
use crate::state::DaemonState;

pub struct Daemon {
    socket_path: PathBuf,
    state_path: PathBuf,
    factory: Arc<dyn AgentFactory>,
    idle_timeout: std::time::Duration,
    quiescence_timeout: std::time::Duration,
    send_guidelines: bool,
    lifecycle_tx: Option<LifecycleEventSender>,
}

impl Daemon {
    pub fn new(state_path: &std::path::Path) -> Self {
        Self {
            socket_path: Self::socket_path(),
            state_path: state_path.to_path_buf(),
            factory: Arc::new(crate::agent::AcprFactory::default()),
            idle_timeout: std::time::Duration::from_secs(900),
            quiescence_timeout: std::time::Duration::from_secs(10),
            send_guidelines: true,
            lifecycle_tx: None,
        }
    }

    pub fn new_with_paths(state_path: &std::path::Path, socket_path: &std::path::Path) -> Self {
        Self {
            socket_path: socket_path.to_path_buf(),
            state_path: state_path.to_path_buf(),
            factory: Arc::new(crate::agent::AcprFactory::default()),
            idle_timeout: std::time::Duration::from_secs(900),
            quiescence_timeout: std::time::Duration::from_secs(10),
            send_guidelines: true,
            lifecycle_tx: None,
        }
    }

    pub fn with_factory(mut self, factory: Arc<dyn AgentFactory>) -> Self {
        self.factory = factory;
        self
    }

    pub fn with_idle_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    pub fn with_quiescence_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.quiescence_timeout = timeout;
        self
    }

    pub fn with_send_guidelines(mut self, send: bool) -> Self {
        self.send_guidelines = send;
        self
    }

    pub fn with_lifecycle_events(mut self, tx: LifecycleEventSender) -> Self {
        self.lifecycle_tx = Some(tx);
        self
    }

    pub fn socket_path() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".jamsession")
            .join("daemon.sock")
    }

    pub async fn run(&self) -> Result<(), Error> {
        let state = DaemonState::load(&self.state_path);

        let (dispatcher_tx, dispatcher_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatcherMessage>();

        // ANCHOR: cwd-health-check-timer
        {
            let tx = dispatcher_tx.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                    let _ = tx.send(DispatcherMessage::CwdHealthCheck);
                }
            });
        }
        // ANCHOR_END: cwd-health-check-timer

        // Prepare socket
        if let Some(parent) = self.socket_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let _ = tokio::fs::remove_file(&self.socket_path).await;
        let listener = UnixListener::bind(&self.socket_path)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o600);
            std::fs::set_permissions(&self.socket_path, perms)?;
        }

        tracing::info!(path = %self.socket_path.display(), "daemon listening");

        if let Some(tx) = &self.lifecycle_tx {
            let _ = tx.send(LifecycleEvent::Initialized);
        }

        // ANCHOR: accept-loop
        {
            let accept_tx = dispatcher_tx.clone();
            tokio::spawn(async move {
                let mut next_client_id = 1u64;
                loop {
                    match listener.accept().await {
                        Ok((stream, _)) => {
                            let client_id = next_client_id;
                            next_client_id += 1;
                            let tx = accept_tx.clone();
                            tokio::spawn(dispatcher::client_pipe(stream, client_id, tx));
                        }
                        Err(e) => {
                            tracing::error!("accept error: {e}");
                            break;
                        }
                    }
                }
            });
        }
        // ANCHOR_END: accept-loop

        // Run dispatcher inside a scope for structured task spawning
        scope(
            async |tasks| {
                let mut dispatcher = Dispatcher::new(
                    tasks,
                    state,
                    self.state_path.clone(),
                    self.factory.clone(),
                    self.idle_timeout,
                    self.quiescence_timeout,
                    self.send_guidelines,
                    self.lifecycle_tx.clone(),
                    dispatcher_tx,
                );

                dispatcher.run(dispatcher_rx).await;
                Ok(())
            },
            scope_tasks::scope_hack!(),
        )
        .await
    }

    pub async fn shutdown(&self) {
        let _ = tokio::fs::remove_file(&self.socket_path).await;
        tracing::info!("daemon shut down");
    }
}
