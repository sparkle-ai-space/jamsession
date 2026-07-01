use std::sync::Arc;
use std::time::Duration;

use jamsession::agent::AcprFactory;
use jamsession::daemon::Daemon;
use jamsession_test::LifecycleEvent;
use tokio::sync::mpsc;

/// Live integration test that spawns a real agent via acpr (claude-acp).
///
/// Requires:
/// - `CLAUDE_CODE_EXECUTABLE` pointing to the claude binary
/// - A valid model configured in `~/.claude/settings.json`
///
/// Run with: `cargo test -p jamsession-test live_agent -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread")]
#[ignore] // requires a real agent (claude-acp via acpr)
async fn live_agent_responds_to_prompt() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init()
        .ok();

    let dir = tempfile::TempDir::new().unwrap();
    let socket_path = dir.path().join("daemon.sock");
    let db_path = dir.path().join("jamsession.db");

    let (lifecycle_tx, mut lifecycle_rx) = mpsc::unbounded_channel();

    let socket_clone = socket_path.clone();
    let daemon_db_path = db_path.clone();
    let _handle = tokio::spawn(async move {
        let daemon = Daemon::new_with_paths(&daemon_db_path, &socket_clone)
            .with_factory(Arc::new(AcprFactory::default()))
            .with_default_model(Some("default".to_string()))
            .with_trace(true)
            .with_send_guidelines(false)
            .with_lifecycle_events(lifecycle_tx);
        let _ = daemon.run().await;
    });

    // Wait for daemon to be ready
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(LifecycleEvent::Initialized) = lifecycle_rx.recv().await {
                break;
            }
        }
    })
    .await
    .expect("daemon did not initialize in time");

    let transport = jamsession_test::transport::UnixSocketTransport::new(&socket_path);
    let result = rhaicp::client::RhaiClient::new()
        .cwd("/tmp")
        .execute(
            transport,
            r#"
                let s = start_session();
                s.prompt("Hi, who is this?")
            "#,
        )
        .await;

    match &result {
        Ok(result) => println!("Agent response: [{result}]"),
        Err(error) => println!("Agent error: {error}"),
    }

    let store = jamsession::db::Store::open(&db_path)
        .await
        .expect("failed to reopen trace database");
    let traces = store
        .traces(jamsession::db::TraceQuery::default())
        .await
        .expect("failed to query traces");
    println!("Trace rows: {}", traces.len());
    for trace in traces {
        println!(
            "#{:03} {:?} {:?} {:?} {:?} id={:?} payload={}",
            trace.id,
            trace.dir,
            trace.role,
            trace.kind,
            trace.method,
            trace.request_id,
            trace.payload
        );
    }

    // The response may be empty if the agent streams via session/update
    // notifications rather than returning text in the prompt response.
    // The key assertion is that we get here without errors.
    result.expect("client script failed");
}
