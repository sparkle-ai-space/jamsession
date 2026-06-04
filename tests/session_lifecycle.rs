use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

async fn start_daemon(
    socket_path: &std::path::Path,
    state_path: &std::path::Path,
) -> tokio::task::JoinHandle<()> {
    let socket_path_clone = socket_path.to_path_buf();
    let state_path = state_path.to_path_buf();
    let handle = tokio::spawn(async move {
        let daemon = academy::daemon::Daemon::new_with_paths(&state_path, &socket_path_clone);
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
    let msg = format!("{}\n", serde_json::to_string(&request).unwrap());
    stream.write_all(msg.as_bytes()).await.unwrap();

    let mut buf = vec![0u8; 16384];
    let n = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut buf))
        .await
        .expect("timeout waiting for response")
        .expect("read error");

    let response_str = std::str::from_utf8(&buf[..n]).unwrap();
    serde_json::from_str(response_str.trim()).unwrap()
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
    assert!(session_id.starts_with("sess_"), "got: {session_id}");
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

    // Verify state file was written
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

    // Create a session first
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

    // Now list sessions
    let list_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "session/list",
        "params": {
            "cwd": "/tmp",
            "cursor": null
        }
    });
    let list_resp = send_request(&mut stream, list_req).await;
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
