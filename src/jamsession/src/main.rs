use std::path::PathBuf;

use clap::{Parser, Subcommand};
use jamsession::daemon::Daemon;
use jamsession::state::DaemonState;

#[derive(Parser)]
#[command(name = "jamsession", about = "Agent daemon for managing ACP sessions")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the daemon (default)
    Daemon {
        /// Path to the state file
        #[arg(long)]
        state_path: Option<PathBuf>,
    },
    /// Run as stdio ACP client (connects to daemon)
    Acp,
}

/// T042+T043: Set up file-based logging to ~/.jamsession/daemon.log
/// and per-session routing to ~/.jamsession/sessions/<id>/session.log
fn init_daemon_logging() {
    use jamsession::logging::SessionFileLayer;
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let log_dir = dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".jamsession");

    let _ = std::fs::create_dir_all(&log_dir);

    let file_appender = tracing_appender::rolling::daily(&log_dir, "daemon.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    // Leak the guard so it lives for the process lifetime
    std::mem::forget(_guard);

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_writer(non_blocking))
        .with(SessionFileLayer::new())
        .init();
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli.command.unwrap_or(Command::Daemon { state_path: None }) {
        Command::Daemon { state_path } => {
            init_daemon_logging();

            let state_path = state_path.unwrap_or_else(DaemonState::state_path);
            let daemon = Daemon::new(&state_path);

            let shutdown = tokio::signal::ctrl_c();
            tokio::select! {
                result = daemon.run() => {
                    if let Err(e) = result {
                        tracing::error!("daemon error: {e}");
                    }
                }
                _ = shutdown => {
                    tracing::info!("received shutdown signal");
                    daemon.shutdown().await;
                }
            }
        }
        Command::Acp => {
            tracing_subscriber::fmt::init();
            run_acp_mode().await;
        }
    }
}

async fn run_acp_mode() {
    let socket_path = Daemon::socket_path();

    // Auto-start daemon if socket doesn't exist
    if !socket_path.exists() {
        tracing::info!("daemon not running, attempting to start...");
        if let Err(e) = auto_start_daemon().await {
            tracing::error!("failed to auto-start daemon: {e}");
            std::process::exit(1);
        }
    }

    // Connect to daemon socket and bridge stdio <-> socket
    match tokio::net::UnixStream::connect(&socket_path).await {
        Ok(stream) => {
            let (mut sock_read, mut sock_write) = stream.into_split();
            let mut stdin = tokio::io::stdin();
            let mut stdout = tokio::io::stdout();

            tokio::select! {
                r = tokio::io::copy(&mut sock_read, &mut stdout) => {
                    if let Err(e) = r { tracing::error!("socket->stdout error: {e}"); }
                }
                r = tokio::io::copy(&mut stdin, &mut sock_write) => {
                    if let Err(e) = r { tracing::error!("stdin->socket error: {e}"); }
                }
            }
        }
        Err(e) => {
            tracing::error!("failed to connect to daemon: {e}");
            std::process::exit(1);
        }
    }
}

async fn auto_start_daemon() -> Result<(), Box<dyn std::error::Error>> {
    let exe = std::env::current_exe()?;
    std::process::Command::new(exe)
        .arg("daemon")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    // Wait for socket to appear
    let socket_path = Daemon::socket_path();
    for _ in 0..50 {
        if socket_path.exists() {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    Err("daemon did not start in time".into())
}
