# Quickstart: Integration Test Harness

## Writing a Basic Integration Test

```rust
// tests/integration/basic.rs
use std::time::Duration;

mod harness;
use harness::{TestDaemon, TestDaemonConfig};

#[tokio::test]
async fn hello_world() {
    // 1. Start an isolated daemon with a simple echo agent
    let daemon = TestDaemon::start(TestDaemonConfig {
        agent_script: r#"
            let prompt = receive_prompt();
            say("echo: " + prompt);
        "#.into(),
        ..Default::default()
    }).await;

    // 2. Run a client script against it
    let result = daemon.execute_client(r#"
        let s = start_session();
        s.prompt("hello")
    "#).await;

    // 3. Assert
    assert_eq!(result, "echo: hello");
}
```

## Agent Script Functions

Available in the agent's Rhai script (runs inside `RhaiAgent`):

| Function | Description |
|----------|-------------|
| `receive_prompt()` → String | Block until a prompt arrives, return its text |
| `say(text)` | Send text to the client as an agent message |
| `user(text)` | Emit a user message (for replay during load) |
| `is_load()` → bool | Whether this is a session load (vs new) |
| `sleep(ms)` | Sleep for N milliseconds |
| `panic(msg)` | Simulate agent crash (panics the agent thread) |
| `cwd()` → String | Get the session's working directory |

## Client Script Functions

Available in the client's Rhai script (runs inside `RhaiClient`):

| Function | Description |
|----------|-------------|
| `start_session()` → Session | Create a new session |
| `load_session(id)` → Session | Load an existing session |
| `resume_session(id)` → Session | Resume a live session |
| `list_sessions()` → Array | List session IDs |
| `sleep(ms)` | Sleep for N milliseconds |

### Session Object Methods

| Method | Description |
|--------|-------------|
| `s.prompt(text)` → String | Send a prompt and return the agent's response text |
| `s.session_id()` → String | Get the session ID |
| `s.updates()` → Array | Get session updates (replay messages, tool calls) |

## Testing Session Lifecycle

```rust
#[tokio::test]
async fn disconnect_and_reconnect() {
    let daemon = TestDaemon::start(TestDaemonConfig {
        agent_script: r#"
            if is_load() {
                user("first prompt");
                say("first response");
            }
            let prompt = receive_prompt();
            say("response: " + prompt);
        "#.into(),
        ..Default::default()
    }).await;

    // Client 1: create session
    let session_id = daemon.execute_client(r#"
        let s = start_session();
        s.prompt("first prompt");
        s.session_id()
    "#).await;

    // Client 2: load session (agent replays, then waits for prompt)
    let result = daemon.execute_client(&format!(r#"
        let s = load_session("{session_id}");
        s.prompt("second prompt")
    "#)).await;

    assert_eq!(result, "response: second prompt");
}
```

## Testing Agent Lifecycle (Idle Timeout)

```rust
#[tokio::test]
async fn agent_spins_down_after_idle() {
    let daemon = TestDaemon::start(TestDaemonConfig {
        idle_timeout: Duration::from_millis(50),
        agent_script: r#"
            let prompt = receive_prompt();
            say("alive: " + prompt);
        "#.into(),
        ..Default::default()
    }).await;

    // Client 1: create session, then disconnect
    let session_id = daemon.execute_client(r#"
        let s = start_session();
        s.prompt("hello");
        s.session_id()
    "#).await;

    // Wait for agent to spin down (quiescence + idle timeout)
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Client 2: load session — agent must respawn
    let result = daemon.execute_client(&format!(r#"
        let s = load_session("{session_id}");
        s.prompt("after respawn")
    "#)).await;

    assert_eq!(result, "alive: after respawn");
}
```

## Running Tests

```bash
# Run all integration tests
cargo test --test '*'

# Run a specific test
cargo test --test session_lifecycle disconnect_and_reconnect

# Run with output (for debugging)
cargo test --test session_lifecycle -- --nocapture
```
