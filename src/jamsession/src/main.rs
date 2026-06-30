use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use jamsession::config::Config;
use jamsession::daemon::Daemon;
use jamsession::db::Store;

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
        /// Path to the SQLite database
        #[arg(long)]
        db_path: Option<PathBuf>,
    },
    /// Run as stdio ACP client (connects to daemon)
    Acp,
    /// Kill a running daemon
    Kill,
}

fn init_daemon_logging(config_dir: &Path, log_filter: Option<&str>) {
    use jamsession::logging::SessionFileLayer;
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let _ = std::fs::create_dir_all(config_dir);

    let file_appender = tracing_appender::rolling::daily(config_dir, "daemon.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    // Leak the guard so it lives for the process lifetime
    std::mem::forget(_guard);

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(log_filter.unwrap_or("info"))
    });

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_writer(non_blocking))
        .with(SessionFileLayer::new_with_base(config_dir.join("sessions")))
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

    match cli.command.unwrap_or(Command::Daemon { db_path: None }) {
        Command::Daemon { db_path } => {
            let config = Config::load(&config_dir);
            for (key, value) in config.daemon_env() {
                // SAFETY: called at startup before spawning threads.
                unsafe { std::env::set_var(key, value) };
            }
            init_daemon_logging(&config_dir, config.log_filter());
            let idle_timeout = config.idle_timeout();
            let quiescence_timeout = config.quiescence_timeout();
            let default_model = config.default_model().map(String::from);
            let factory = config.into_factory();

            let db_path = db_path.unwrap_or_else(|| config_dir.join("jamsession.db"));
            let socket_path = config_dir.join("daemon.sock");

            write_pid_file(&config_dir);

            let store = match Store::open(&db_path).await {
                Ok(store) => store,
                Err(e) => {
                    tracing::error!("failed to open database: {e}");
                    std::process::exit(1);
                }
            };

            let daemon = Daemon::new_with_store(store, &socket_path)
                .with_factory(factory)
                .with_idle_timeout(idle_timeout)
                .with_quiescence_timeout(quiescence_timeout)
                .with_default_model(default_model);

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
        Command::Kill => {
            kill_daemon(&config_dir);
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

fn write_pid_file(config_dir: &Path) {
    let pid_path = config_dir.join("daemon.pid");
    let _ = std::fs::write(&pid_path, std::process::id().to_string());
}

fn kill_daemon(config_dir: &Path) {
    let pid_path = config_dir.join("daemon.pid");
    let pid_str = match std::fs::read_to_string(&pid_path) {
        Ok(s) => s,
        Err(_) => {
            eprintln!("no daemon.pid found in {}", config_dir.display());
            std::process::exit(1);
        }
    };

    let pid: i32 = match pid_str.trim().parse() {
        Ok(p) => p,
        Err(_) => {
            eprintln!("invalid pid in {}", pid_path.display());
            std::process::exit(1);
        }
    };

    #[allow(unsafe_code)]
    let result = unsafe { libc::kill(pid, libc::SIGTERM) };

    if result == 0 {
        eprintln!("sent SIGTERM to daemon (pid {pid})");
        let _ = std::fs::remove_file(&pid_path);
    } else {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ESRCH) {
            eprintln!("daemon not running (stale pid file)");
            let _ = std::fs::remove_file(&pid_path);
            let _ = std::fs::remove_file(config_dir.join("daemon.sock"));
        } else {
            eprintln!("failed to kill daemon (pid {pid}): {err}");
            std::process::exit(1);
        }
    }
}
