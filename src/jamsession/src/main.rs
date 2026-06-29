use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use jamsession::config::Config;
use jamsession::daemon::Daemon;

#[derive(Parser)]
#[command(name = "jamsession", about = "Agent daemon for managing ACP sessions")]
struct Cli {
    /// Override the config/data directory (default: ~/.jamsession)
    #[arg(long, global = true)]
    config_dir: Option<PathBuf>,

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

fn default_config_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".jamsession")
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let config_dir = cli.config_dir.unwrap_or_else(default_config_dir);

    match cli.command.unwrap_or(Command::Daemon { state_path: None }) {
        Command::Daemon { state_path } => {
            init_daemon_logging();

            let config = Config::load(&config_dir);
            let factory = config.into_factory();

            let state_path = state_path.unwrap_or_else(|| config_dir.join("state.json"));
            let socket_path = config_dir.join("daemon.sock");

            let daemon = Daemon::new_with_paths(&state_path, &socket_path).with_factory(factory);

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
            run_acp_mode(&config_dir).await;
        }
    }
}

async fn run_acp_mode(config_dir: &Path) {
    let socket_path = config_dir.join("daemon.sock");

    // Auto-start daemon if socket doesn't exist
    if !socket_path.exists() {
        tracing::info!("daemon not running, attempting to start...");
        if let Err(e) = auto_start_daemon(config_dir, &socket_path).await {
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

async fn auto_start_daemon(
    config_dir: &Path,
    socket_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let exe = std::env::current_exe()?;
    let mut cmd = std::process::Command::new(exe);
    cmd.arg("--config-dir")
        .arg(config_dir)
        .arg("daemon")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    cmd.spawn()?;

    // Wait for socket to appear
    for _ in 0..50 {
        if socket_path.exists() {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    Err("daemon did not start in time".into())
}
