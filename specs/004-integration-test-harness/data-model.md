# Data Model: Integration Test Harness

## Architecture Overview

The test harness sits between the integration test code and the daemon. It provides a `TestDaemon` that starts an isolated daemon instance with an injected `RhaiAgent` factory, and leverages `RhaiClient` from rhaicp to drive scripted client connections against it.

```text
┌─────────────────────────────────────────────────────────────┐
│ Integration Test (Rust #[tokio::test])                       │
│                                                             │
│   TestDaemon::start(config)                                 │
│     │                                                       │
│     ├── Daemon (isolated socket + state dir)                │
│     │     └── AgentFactory: RhaiAgentFactory                │
│     │           └── creates RhaiAgent in-process per session│
│     │                                                       │
│     └── RhaiClient::execute(transport, script)              │
│           └── connects to daemon socket as ACP client        │
│                                                             │
│   Assertions on responses / session updates                 │
└─────────────────────────────────────────────────────────────┘
```

## Key Integration Points

### Agent Factory Abstraction (daemon change)

The daemon's agent spawning is abstracted behind a trait:

```rust
#[trait_variant::make(Send)]
pub trait AgentFactory: Send + Sync + 'static {
    async fn spawn_agent(
        &self,
        session_id: &str,
        cwd: &Path,
        mcp_servers: Vec<McpServer>,
    ) -> Result<Box<dyn ConnectTo<Client>>, Error>;
}
```

Production: `AcprFactory` wraps `acpr::Acpr::new("claude-acp")`.
Tests: `RhaiAgentFactory` creates `RhaiAgent` instances configured with test scripts.

### RhaiClient Connection to Daemon Socket

`RhaiClient::execute()` accepts `impl ConnectTo<Client>`. The test harness provides a `UnixSocketTransport` that:
1. Connects to the daemon's Unix socket
2. Wraps it into `ByteStreams` (read/write halves with compat layer)
3. Implements `ConnectTo<Client>` so the daemon sees it as a normal ACP client

---

## Entities

### TestDaemon

Test utility struct that starts and manages an isolated daemon instance.

| Field | Type | Description |
|-------|------|-------------|
| temp_dir | `TempDir` | Owns isolated state + socket directory |
| socket_path | `PathBuf` | Path to daemon's Unix socket |
| daemon_handle | `JoinHandle<()>` | Tokio task running the daemon |
| agent_factory | `Arc<RhaiAgentFactory>` | Factory for creating test agents |

**Lifecycle**: Created at test start, dropped at test end (Drop impl kills daemon task, cleans up temp dir).

### TestDaemonConfig

Configuration for a test daemon instance.

| Field | Type | Description |
|-------|------|-------------|
| idle_timeout | `Duration` | Agent idle timeout (default: 5min, tests use 50ms) |
| agent_script | `String` | Rhai script for the agent (used for all sessions) |
| per_session_scripts | `HashMap<String, String>` | Optional per-session scripts (keyed by session ID pattern) |

### RhaiAgentFactory

Test implementation of `AgentFactory` that creates in-process `RhaiAgent` instances.

| Field | Type | Description |
|-------|------|-------------|
| new_session_script | `Option<String>` | Default script for new sessions |
| prior_sessions | `Vec<PriorSession>` | Prior session configurations for load/resume |
| (no special config needed — agents use `panic(msg)` to simulate crashes) | | |

### UnixSocketTransport

Adapter that connects to a Unix socket and implements `ConnectTo<Client>`.

| Field | Type | Description |
|-------|------|-------------|
| socket_path | `PathBuf` | Path to daemon's Unix socket |

---

## Test Flow

### Basic Session Test

```rust
#[tokio::test]
async fn basic_session_prompt_response() {
    let daemon = TestDaemon::start(TestDaemonConfig {
        agent_script: r#"
            let prompt = receive_prompt();
            say("echo: " + prompt);
        "#.into(),
        ..Default::default()
    }).await;

    let result = daemon.execute_client(r#"
        let s = start_session();
        s.prompt("hello")
    "#).await;

    assert_eq!(result, "echo: hello");
}
```

### Session Lifecycle Test

```rust
#[tokio::test]
async fn load_session_replays_history() {
    let daemon = TestDaemon::start(TestDaemonConfig {
        agent_script: r#"
            if is_load() {
                user("original prompt");
                say("original response");
            }
            let prompt = receive_prompt();
            say("new: " + prompt);
        "#.into(),
        ..Default::default()
    }).await;

    // First client: create session
    let session_id = daemon.execute_client(r#"
        let s = start_session();
        s.prompt("original prompt");
        s.session_id()
    "#).await;

    // Second client: load session (triggers replay)
    let result = daemon.execute_client(&format!(r#"
        let s = load_session("{session_id}");
        let updates = s.updates();  // contains replay
        s.prompt("follow up")
    "#)).await;

    assert_eq!(result, "new: follow up");
}
```

---

## Relationships

```
TestDaemon 1──1 Daemon (isolated instance)
TestDaemon 1──1 RhaiAgentFactory
TestDaemon 1──* RhaiClient (sequential execute() calls)
RhaiAgentFactory 1──* RhaiAgent (one per session activation)
Daemon 1──1 AgentFactory (trait object)
```

---

## Validation Rules

- `TestDaemon::start()` must complete (socket exists) within 2 seconds or panic
- Each test gets a unique temp dir — no shared state between tests
- Agent scripts use `panic(msg)` to simulate crashes (never `exit()`)
- Drop guards ensure cleanup even on test panics

## Required Changes to Existing Code

### Daemon (src/daemon.rs, src/agent.rs)
1. Extract `AgentFactory` trait
2. Add `AgentFactory` field to `Daemon` struct (or pass to `SessionManager`)
3. Replace hardcoded `AgentManager::spawn_for_session()` with factory call
4. Add `Daemon::new_with_factory()` constructor for tests

### rhaicp (external dependency)
1. Add `panic(msg)` function to the Rhai engine (panics the agent thread)
2. Deprecate/remove `exit(code)` — unsafe in all contexts

### New Code (tests/ directory)
1. `tests/harness/mod.rs` — `TestDaemon`, `TestDaemonConfig`, `RhaiAgentFactory`
2. `tests/harness/transport.rs` — `UnixSocketTransport` implementing `ConnectTo<Client>`
3. Integration test files using the harness
