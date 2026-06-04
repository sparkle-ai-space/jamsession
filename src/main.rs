use std::path::PathBuf;

use academy::daemon::Daemon;
use academy::state::DaemonState;

#[derive(Debug)]
enum Command {
    Daemon { state_path: Option<PathBuf> },
    Acp,
}

fn parse_args() -> Command {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("daemon") => Command::Daemon {
            state_path: args.get(2).map(PathBuf::from),
        },
        Some("acp") => Command::Acp,
        Some("--help") | Some("-h") => {
            print_help();
            std::process::exit(0);
        }
        _ => {
            // Default to daemon mode
            Command::Daemon { state_path: None }
        }
    }
}

fn print_help() {
    eprintln!(
        "academy - Agent daemon for managing ACP sessions\n\
         \n\
         USAGE:\n\
         \x20   academy [COMMAND]\n\
         \n\
         COMMANDS:\n\
         \x20   daemon    Run the daemon (default)\n\
         \x20   acp       Run as stdio ACP client (connects to daemon)\n\
         \n\
         OPTIONS:\n\
         \x20   -h, --help    Show this help message"
    );
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    match parse_args() {
        Command::Daemon { state_path } => {
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
