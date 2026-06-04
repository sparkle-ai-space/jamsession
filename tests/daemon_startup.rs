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

    // Wait for socket to appear
    for _ in 0..50 {
        if socket_path.exists() {
            return handle;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("daemon did not start in time");
}

#[tokio::test]
async fn daemon_creates_socket_file() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket_path = dir.path().join("daemon.sock");
    let state_path = dir.path().join("state.json");

    let _handle = start_daemon(&socket_path, &state_path).await;
    assert!(socket_path.exists());
}

#[tokio::test]
async fn daemon_accepts_connection_and_responds_to_initialize() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket_path = dir.path().join("daemon.sock");
    let state_path = dir.path().join("state.json");

    let _handle = start_daemon(&socket_path, &state_path).await;

    let mut stream = UnixStream::connect(&socket_path).await.unwrap();

    // Send initialize request (JSON-RPC)
    let init_request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "1",
            "clientCapabilities": {},
            "clientInfo": {"name": "test", "title": "Test Client", "version": "0.1.0"}
        }
    });
    let msg = format!("{}\n", serde_json::to_string(&init_request).unwrap());
    stream.write_all(msg.as_bytes()).await.unwrap();

    // Read response (with timeout)
    let mut buf = vec![0u8; 8192];
    let n = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut buf))
        .await
        .expect("timeout waiting for response")
        .expect("read error");

    let response_str = std::str::from_utf8(&buf[..n]).unwrap();
    let response: serde_json::Value = serde_json::from_str(response_str.trim()).unwrap();

    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 1);
    // The response will be an error since we can't spawn a real agent in tests,
    // but the daemon properly handled the request.
    assert!(
        response.get("result").is_some() || response.get("error").is_some(),
        "Expected result or error in response: {response}"
    );
}

#[tokio::test]
async fn session_list_returns_empty() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket_path = dir.path().join("daemon.sock");
    let state_path = dir.path().join("state.json");

    let _handle = start_daemon(&socket_path, &state_path).await;

    let mut stream = UnixStream::connect(&socket_path).await.unwrap();

    let list_request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "session/list",
        "params": {
            "cwd": null,
            "cursor": null
        }
    });
    let msg = format!("{}\n", serde_json::to_string(&list_request).unwrap());
    stream.write_all(msg.as_bytes()).await.unwrap();

    let mut buf = vec![0u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("timeout")
        .expect("read error");

    let response_str = std::str::from_utf8(&buf[..n]).unwrap();
    let response: serde_json::Value = serde_json::from_str(response_str.trim()).unwrap();

    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 1);
    let result = response.get("result").expect("expected result");
    assert_eq!(result["sessions"], serde_json::json!([]));
}
