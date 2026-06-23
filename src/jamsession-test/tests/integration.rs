use std::time::Duration;

use jamsession_test::{LifecycleEvent, TestDaemon, TestDaemonConfig, expect_test::expect};

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
        s.prompt("world")
    "#
        ))
        .await;

    assert_eq!(result, "second: world");
}

#[tokio::test(flavor = "multi_thread")]
async fn load_dead_session_respawns_agent() {
    let daemon = TestDaemon::start(TestDaemonConfig {
        idle_timeout: Duration::from_millis(50),
        agent_script: r#"
            if is_load() {
                user("original prompt");
                say("original response");
            }
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
        s.prompt("follow up")
    "#
        ))
        .await;

    assert_eq!(result, "new: follow up");
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
