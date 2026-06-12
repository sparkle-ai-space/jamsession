use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use jamsession::agent::BinaryFactory;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

fn mock_agent_binary() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_BIN_EXE_mock-agent"));
    if !path.exists() {
        path = PathBuf::from("target/debug/mock-agent");
    }
    path
}

async fn start_daemon(
    socket_path: &std::path::Path,
    state_path: &std::path::Path,
) -> tokio::task::JoinHandle<()> {
    let socket_path_clone = socket_path.to_path_buf();
    let state_path = state_path.to_path_buf();
    let mock_binary = mock_agent_binary();
    let handle = tokio::spawn(async move {
        let daemon = jamsession::daemon::Daemon::new_with_paths(&state_path, &socket_path_clone)
            .with_factory(Arc::new(BinaryFactory::new(mock_binary)));
        let _ = daemon.run().await;
    });

    for _ in 0..50 {
        if socket_path.exists() {
            return handle;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("daemon did not start in time");
}

async fn send_request(stream: &mut UnixStream, request: serde_json::Value) -> serde_json::Value {
    let expected_id = request.get("id").cloned();
    let msg = format!("{}\n", serde_json::to_string(&request).unwrap());
    stream.write_all(msg.as_bytes()).await.unwrap();

    let mut accumulated = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    loop {
        let mut buf = vec![0u8; 16384];
        let n = tokio::time::timeout_at(deadline, stream.read(&mut buf))
            .await
            .expect("timeout waiting for response")
            .expect("read error");

        accumulated.push_str(std::str::from_utf8(&buf[..n]).unwrap());

        // Look for the response matching our request ID
        for line in accumulated.lines() {
            if line.is_empty() {
                continue;
            }
            if let Ok(msg) = serde_json::from_str::<serde_json::Value>(line)
                && msg.get("id") == expected_id.as_ref()
            {
                return msg;
            }
        }
    }
}

#[tokio::test]
async fn new_session_creates_session_and_returns_id() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket_path = dir.path().join("daemon.sock");
    let state_path = dir.path().join("state.json");

    let _handle = start_daemon(&socket_path, &state_path).await;

    let mut stream = UnixStream::connect(&socket_path).await.unwrap();

    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "session/new",
        "params": {
            "cwd": "/tmp",
            "additionalDirectories": [],
            "mcpServers": []
        }
    });

    let response = send_request(&mut stream, request).await;
    assert_eq!(response["id"], 1);

    let result = response.get("result").expect("expected result");
    let session_id = result["sessionId"].as_str().unwrap();
    // T041: session ID now comes from the agent (mock returns "mock_sess_<cwd>")
    assert!(!session_id.is_empty(), "got empty session id");
}

#[tokio::test]
async fn new_session_persists_to_state_file() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket_path = dir.path().join("daemon.sock");
    let state_path = dir.path().join("state.json");

    let _handle = start_daemon(&socket_path, &state_path).await;

    let mut stream = UnixStream::connect(&socket_path).await.unwrap();

    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "session/new",
        "params": {
            "cwd": "/tmp",
            "additionalDirectories": [],
            "mcpServers": []
        }
    });

    let response = send_request(&mut stream, request).await;
    let session_id = response["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    let state_contents = std::fs::read_to_string(&state_path).unwrap();
    let state: serde_json::Value = serde_json::from_str(&state_contents).unwrap();
    let sessions = state["sessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0]["session_id"], session_id);
}

#[tokio::test]
async fn session_list_shows_created_session() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket_path = dir.path().join("daemon.sock");
    let state_path = dir.path().join("state.json");

    let _handle = start_daemon(&socket_path, &state_path).await;

    let mut stream = UnixStream::connect(&socket_path).await.unwrap();

    let create_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "session/new",
        "params": {
            "cwd": "/tmp",
            "additionalDirectories": [],
            "mcpServers": []
        }
    });
    let create_resp = send_request(&mut stream, create_req).await;
    let session_id = create_resp["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    // Need a new connection since bridge is installed on the first one
    let mut stream2 = UnixStream::connect(&socket_path).await.unwrap();
    let list_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "session/list",
        "params": {
            "cwd": "/tmp",
            "cursor": null
        }
    });
    let list_resp = send_request(&mut stream2, list_req).await;
    let sessions = list_resp["result"]["sessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0]["sessionId"], session_id);
}

#[tokio::test]
async fn load_nonexistent_session_returns_error() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket_path = dir.path().join("daemon.sock");
    let state_path = dir.path().join("state.json");

    let _handle = start_daemon(&socket_path, &state_path).await;

    let mut stream = UnixStream::connect(&socket_path).await.unwrap();

    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "session/load",
        "params": {
            "sessionId": "sess_nonexistent",
            "cwd": "/tmp",
            "mcpServers": []
        }
    });

    let response = send_request(&mut stream, request).await;
    assert!(
        response.get("error").is_some(),
        "expected error: {response}"
    );
}

#[tokio::test]
async fn new_session_with_invalid_cwd_returns_error() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket_path = dir.path().join("daemon.sock");
    let state_path = dir.path().join("state.json");

    let _handle = start_daemon(&socket_path, &state_path).await;

    let mut stream = UnixStream::connect(&socket_path).await.unwrap();

    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "session/new",
        "params": {
            "cwd": "/nonexistent/path/that/does/not/exist",
            "additionalDirectories": [],
            "mcpServers": []
        }
    });

    let response = send_request(&mut stream, request).await;
    assert!(
        response.get("error").is_some(),
        "expected error: {response}"
    );
}

#[tokio::test]
async fn load_session_after_create() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket_path = dir.path().join("daemon.sock");
    let state_path = dir.path().join("state.json");

    let _handle = start_daemon(&socket_path, &state_path).await;

    // Create session on first connection
    let mut stream1 = UnixStream::connect(&socket_path).await.unwrap();
    let create_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "session/new",
        "params": {
            "cwd": "/tmp",
            "additionalDirectories": [],
            "mcpServers": []
        }
    });
    let create_resp = send_request(&mut stream1, create_req).await;
    let session_id = create_resp["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    drop(stream1);

    // Load session on second connection
    let mut stream2 = UnixStream::connect(&socket_path).await.unwrap();
    let load_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "session/load",
        "params": {
            "sessionId": session_id,
            "cwd": "/tmp",
            "mcpServers": []
        }
    });
    let load_resp = send_request(&mut stream2, load_req).await;
    assert!(
        load_resp.get("result").is_some(),
        "expected result, got: {load_resp}"
    );
}

#[tokio::test]
async fn resume_session_after_create() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket_path = dir.path().join("daemon.sock");
    let state_path = dir.path().join("state.json");

    let _handle = start_daemon(&socket_path, &state_path).await;

    // Create session
    let mut stream1 = UnixStream::connect(&socket_path).await.unwrap();
    let create_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "session/new",
        "params": {
            "cwd": "/tmp",
            "additionalDirectories": [],
            "mcpServers": []
        }
    });
    let create_resp = send_request(&mut stream1, create_req).await;
    let session_id = create_resp["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    drop(stream1);

    // Resume session
    let mut stream2 = UnixStream::connect(&socket_path).await.unwrap();
    let resume_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "session/resume",
        "params": {
            "sessionId": session_id,
            "cwd": "/tmp",
            "mcpServers": []
        }
    });
    let resume_resp = send_request(&mut stream2, resume_req).await;
    assert!(
        resume_resp.get("result").is_some(),
        "expected result, got: {resume_resp}"
    );
}
