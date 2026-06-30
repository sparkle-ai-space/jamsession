use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use jamsession_test::{
    LifecycleEvent, RhaiAgentFactory, TestDaemon, TestDaemonConfig, TestUnixSocketTransport,
    expect_test::expect,
};

async fn start_file_backed_daemon(
    config: TestDaemonConfig,
    db_path: &Path,
    socket_path: &Path,
) -> tokio::task::JoinHandle<Result<(), jamsession::error::Error>> {
    let factory: Arc<dyn jamsession::agent::AgentFactory> =
        Arc::new(RhaiAgentFactory::new(&config));
    let db_path = db_path.to_path_buf();
    let socket_path = socket_path.to_path_buf();
    let poll_socket_path = socket_path.clone();
    let idle_timeout = config.idle_timeout;

    let handle = tokio::spawn(async move {
        let daemon = jamsession::daemon::Daemon::new_with_paths(&db_path, &socket_path)
            .with_factory(factory)
            .with_idle_timeout(idle_timeout)
            .with_quiescence_timeout(Duration::from_millis(10))
            .with_send_guidelines(false);
        daemon.run().await
    });

    for _ in 0..50 {
        if poll_socket_path.exists() {
            return handle;
        }
        if handle.is_finished() {
            let result = handle.now_or_never().expect("finished handle");
            panic!("daemon exited before creating socket: {result:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("daemon did not start in time");
}

#[tokio::test]
async fn smoke_list_sessions_empty() {
    let daemon = TestDaemon::start(TestDaemonConfig::default()).await;

    let result = daemon
        .execute_client(
            r#"
        let sessions = list_sessions();
        sessions.len()
    "#,
        )
        .await;

    assert_eq!(result, "0");
}

#[tokio::test]
async fn basic_session_prompt_response() {
    let daemon = TestDaemon::start(TestDaemonConfig {
        agent_script: r#"
            let prompt = receive_prompt();
            say("echo: " + prompt);
        "#
        .into(),
        ..Default::default()
    })
    .await;

    let result = daemon
        .execute_client(
            r#"
        let s = start_session();
        s.prompt("hello")
    "#,
        )
        .await;

    assert_eq!(result, "echo: hello");

    daemon
        .wait_for(
            |e| matches!(e, LifecycleEvent::SessionCreated { .. }),
            Duration::from_secs(2),
        )
        .await;

    daemon.assert_lifecycle_events(expect![[r#"
        Initialized
        ClientConnected
        SessionCreated($session0)"#]]);
}

#[tokio::test]
async fn list_sessions_shows_created_session() {
    let daemon = TestDaemon::start(TestDaemonConfig {
        agent_script: r#"
            let prompt = receive_prompt();
            say("ok");
        "#
        .into(),
        ..Default::default()
    })
    .await;

    let session_id = daemon
        .execute_client(
            r#"
        let s = start_session();
        s.prompt("hi");
        s.session_id()
    "#,
        )
        .await;

    assert!(!session_id.is_empty());

    let count = daemon
        .execute_client(
            r#"
        let sessions = list_sessions();
        sessions.len()
    "#,
        )
        .await;

    assert_eq!(count, "1");
}

#[tokio::test]
async fn multiple_sessions_independent() {
    let daemon = TestDaemon::start(TestDaemonConfig {
        agent_script: r#"
            let prompt = receive_prompt();
            say("got: " + prompt);
        "#
        .into(),
        ..Default::default()
    })
    .await;

    let result1 = daemon
        .execute_client(
            r#"
        let s = start_session();
        s.prompt("alpha")
    "#,
        )
        .await;

    let result2 = daemon
        .execute_client(
            r#"
        let s = start_session();
        s.prompt("beta")
    "#,
        )
        .await;

    assert_eq!(result1, "got: alpha");
    assert_eq!(result2, "got: beta");

    let count = daemon
        .execute_client(
            r#"
        let sessions = list_sessions();
        sessions.len()
    "#,
        )
        .await;
    assert_eq!(count, "2");
}

#[tokio::test]
async fn resume_live_session_bridges_immediately() {
    let daemon = TestDaemon::start(TestDaemonConfig {
        agent_script: r#"
            let prompt = receive_prompt();
            say("first: " + prompt);
            let prompt2 = receive_prompt();
            say("resumed: " + prompt2);
        "#
        .into(),
        ..Default::default()
    })
    .await;

    let session_id = daemon
        .execute_client(
            r#"
        let s = start_session();
        s.prompt("hello");
        s.session_id()
    "#,
        )
        .await;

    let result = daemon
        .execute_client(&format!(
            r#"
        let s = resume_session("{session_id}");
        s.prompt("continue")
    "#
        ))
        .await;

    assert_eq!(result, "resumed: continue");
}

#[tokio::test]
async fn load_live_session_replays_buffer() {
    let daemon = TestDaemon::start(TestDaemonConfig {
        agent_script: r#"
            let prompt = receive_prompt();
            say("first: " + prompt);
            loop {
                let prompt2 = receive_prompt();
                if prompt2 != "" {
                    say("second: " + prompt2);
                    break;
                }
            }
        "#
        .into(),
        ..Default::default()
    })
    .await;

    let session_id = daemon
        .execute_client(
            r#"
        let s = start_session();
        s.prompt("hello");
        s.session_id()
    "#,
        )
        .await;

    let result = daemon
        .execute_client(&format!(
            r#"
        let s = load_session("{session_id}");
        let u = s.updates();
        let r = s.prompt("world");
        u[0].type + ":" + u[0].text + "|" + r
    "#
        ))
        .await;

    assert_eq!(result, "agent_message_chunk:first: hello|second: world");
}

#[tokio::test(flavor = "multi_thread")]
async fn load_dead_session_respawns_agent_and_replays_history() {
    let daemon = TestDaemon::start(TestDaemonConfig {
        idle_timeout: Duration::from_millis(50),
        agent_script: r#"
            loop {
                let prompt = receive_prompt();
                if prompt != "" {
                    say("new: " + prompt);
                    break;
                }
            }
        "#
        .into(),
        ..Default::default()
    })
    .await;

    let session_id = daemon
        .execute_client(
            r#"
        let s = start_session();
        s.prompt("original prompt");
        s.session_id()
    "#,
        )
        .await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let result = daemon
        .execute_client(&format!(
            r#"
        let s = load_session("{session_id}");
        let u = s.updates();
        let r = s.prompt("follow up");
        u[0].type + ":" + u[0].text + "|" + r
    "#
        ))
        .await;

    assert_eq!(
        result,
        "agent_message_chunk:new: original prompt|new: follow up"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn load_session_replays_history_after_daemon_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("jamsession.db");
    let socket_path = dir.path().join("daemon.sock");
    let config = || TestDaemonConfig {
        agent_script: r#"
            loop {
                let prompt = receive_prompt();
                if prompt != "" {
                    say("reply: " + prompt);
                    break;
                }
            }
        "#
        .into(),
        ..Default::default()
    };

    let first = start_file_backed_daemon(config(), &db_path, &socket_path).await;
    let session_id = rhaicp::client::RhaiClient::new()
        .cwd("/tmp")
        .execute(
            TestUnixSocketTransport::new(&socket_path),
            r#"
                let s = start_session();
                s.prompt("before restart");
                s.session_id()
            "#,
        )
        .await
        .expect("first client failed");

    first.abort();
    let _ = first.await;
    let _ = tokio::fs::remove_file(&socket_path).await;

    let second = start_file_backed_daemon(config(), &db_path, &socket_path).await;
    let result = rhaicp::client::RhaiClient::new()
        .cwd("/tmp")
        .execute(
            TestUnixSocketTransport::new(&socket_path),
            &format!(
                r#"
                let sessions = list_sessions();
                let s = load_session("{session_id}");
                let u = s.updates();
                let r = s.prompt("after restart");
                sessions.len() + "|" + u[0].type + ":" + u[0].text + "|" + r
            "#
            ),
        )
        .await
        .expect("second client failed");

    assert_eq!(
        result,
        "1|agent_message_chunk:reply: before restart|reply: after restart"
    );

    second.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn agent_killed_after_idle_timeout() {
    let daemon = TestDaemon::start(TestDaemonConfig {
        idle_timeout: Duration::from_millis(50),
        agent_script: r#"
            if is_load() {}
            loop {
                let prompt = receive_prompt();
                if prompt != "" {
                    say("alive: " + prompt);
                    break;
                }
            }
        "#
        .into(),
        ..Default::default()
    })
    .await;

    let session_id = daemon
        .execute_client(
            r#"
        let s = start_session();
        s.prompt("hello");
        s.session_id()
    "#,
        )
        .await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let result = daemon
        .execute_client(&format!(
            r#"
        let s = load_session("{session_id}");
        s.prompt("after respawn")
    "#
        ))
        .await;

    assert_eq!(result, "alive: after respawn");
}

#[tokio::test(flavor = "multi_thread")]
async fn agent_crash_detected_and_reload_works() {
    let daemon = TestDaemon::start(TestDaemonConfig {
        agent_script: r#"
            if is_load() {}
            loop {
                let prompt = receive_prompt();
                if prompt != "" {
                    say("response: " + prompt);
                    break;
                }
            }
        "#
        .into(),
        // Agent connection will be forcibly closed 200ms after spawn
        crash_after: Some(Duration::from_millis(200)),
        ..Default::default()
    })
    .await;

    let session_id = daemon
        .execute_client(
            r#"
        let s = start_session();
        s.prompt("hello");
        s.session_id()
    "#,
        )
        .await;

    // Wait for the agent to crash (time bomb fires after 200ms)
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Load the session — should detect dead agent and spawn a new one
    let result = daemon
        .execute_client(&format!(
            r#"
        let s = load_session("{session_id}");
        s.prompt("after crash")
    "#
        ))
        .await;

    assert_eq!(result, "response: after crash");
}

#[tokio::test]
async fn direct_rhaiagent_load_session() {
    use agent_client_protocol::schema::SessionId;
    use jamsession_test::PriorSession;
    use rhaicp::RhaiAgent;

    let session_id = "test-session-123";
    let script = r#"
        if is_load() {
            user("history");
            say("replayed");
        }
        loop {
            let prompt = receive_prompt();
            if prompt != "" {
                say("response: " + prompt);
                break;
            }
        }
    "#;

    let agent = RhaiAgent::new().prior_sessions(vec![PriorSession {
        session_id: SessionId::new(session_id),
        script: script.to_string(),
    }]);

    let result = rhaicp::client::RhaiClient::new()
        .cwd("/tmp")
        .execute(
            agent,
            &format!(
                r#"
        let s = load_session("{session_id}");
        let updates = s.updates();
        let r = s.prompt("hello");
        "updates=" + updates.len() + " result=[" + r + "]"
    "#
            ),
        )
        .await
        .expect("client execute failed");

    assert!(
        result.contains("result=[response: hello]"),
        "unexpected: {result}"
    );
}
