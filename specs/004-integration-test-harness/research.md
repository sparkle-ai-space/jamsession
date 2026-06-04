# Research: Integration Test Harness

## R1. In-Process Agent Injection

**Decision**: Make the daemon's agent spawn function configurable via a trait object / closure. In tests, inject a factory that creates `RhaiAgent` instances in-process (using the rhaicp library API).

**Rationale**: The `RhaiAgent` from rhaicp 2.1 implements `ConnectTo<Client>`, making it a drop-in replacement for the production agent transport (acpr). The daemon's `AgentManager` currently hardcodes `acpr::Acpr::new("claude-acp")`. By abstracting agent creation behind a trait/closure that receives session context (session ID, cwd), tests can inject `RhaiAgent::new().new_session_script(script)` without any filesystem or network dependencies.

**Alternatives considered**:
- Mock agent binary — requires building/spawning a separate process, slower, more fragile
- Test-only feature flag — couples test infrastructure to conditional compilation, harder to maintain
- In-memory transport pair — possible but `RhaiAgent` already implements the right trait

## R2. RhaiClient for Test Scripting

**Decision**: Use `rhaicp::client::RhaiClient` as the test client. Each `RhaiClient::execute()` call connects to the daemon's Unix socket, runs a Rhai script, and disconnects.

**Rationale**: `RhaiClient` implements `ConnectTo<Client>` pattern internally — it opens a connection, runs the script to completion, and returns. The test harness controls the sequence of connections by calling `RhaiClient::new().cwd(dir).execute(transport, script)` multiple times against the same daemon socket. This maps directly to the spec's model of sequential client connections.

**Alternatives considered**:
- Raw Unix socket + hand-rolled JSON-RPC — exactly what the existing tests do, but verbose and error-prone
- Custom test client — unnecessary when rhaicp already provides the right abstraction

## R3. RhaiClient Transport — ConnectTo<Client> for Unix Sockets

**Decision**: Use `agent_client_protocol::ByteStreams` over a `UnixStream` split into read/write halves (with `tokio_util::compat`). Wrap this into a `ConnectTo<Client>` adapter that `RhaiClient::execute()` can accept.

**Rationale**: `RhaiClient::execute()` takes `impl ConnectTo<Client>`, which is the agent-side trait. The daemon listens on a Unix socket and acts as an Agent. So the client needs a transport that implements `ConnectTo<Client>` by connecting to the Unix socket. The `ByteStreams` type from agent-client-protocol handles the framing; we just need to wrap `UnixStream` appropriately.

**Alternatives considered**:
- Pipe-based in-memory transport — would bypass the Unix socket, defeating the purpose of integration testing
- Shared in-process channels — same issue, doesn't test the real daemon path

## R4. Test Isolation Strategy

**Decision**: Each test creates its own `tempfile::TempDir` containing the socket file and state file. The `TestDaemon` struct owns the temp dir and implements `Drop` for cleanup.

**Rationale**: This is what the existing tests already do. `tempfile::TempDir` guarantees unique paths and auto-cleanup. Unix socket path length limit (108 bytes) is not a concern since temp dirs are short paths. Multiple tests run in parallel by default with `cargo test` and cannot interfere.

**Alternatives considered**:
- Shared daemon with per-test namespacing — complex, introduces coupling between tests
- Docker/container isolation — overkill for unit-test-speed integration tests

## R5. Agent Crash Simulation via `panic()` Function

**Decision**: Add a new `panic(msg)` function to rhaicp's Rhai engine that panics the agent thread. Deprecate or remove `exit(code)` — it's too dangerous in practice (kills the entire process whether in-process or not).

**Rationale**: A dedicated `panic(msg)` function clearly communicates intent ("simulate a crash") and is safe in all contexts. When the agent runs in-process, `spawn_blocking` catches the panic as a `JoinError` and the daemon sees the connection drop — mapping naturally to "agent process died". When running as a subprocess, a panic still terminates the process (via abort or unwind), so behavior is consistent. Meanwhile `exit()` is hazardous: even in production it bypasses destructors and cleanup.

**Alternatives considered**:
- Configurable `exit()` (panic in tests, real exit in prod) — adds mode-dependent behavior, confusing semantics
- Keep `exit()` alongside `panic()` — `exit()` has no safe use case; better to remove the footgun
- Custom Rhai error type — doesn't propagate through the channel/connection boundary cleanly

## R6. Agent Factory Trait Design

**Decision**: Define a trait `AgentFactory` with a single async method `spawn_agent(session_id, cwd, mcp_servers) -> Result<impl ConnectTo<Client>>` (or use a boxed closure `Box<dyn Fn(...) -> BoxFuture<...>>`). The daemon holds an `Arc<dyn AgentFactory>` that defaults to `AcprFactory` in production and `RhaiAgentFactory` in tests.

**Rationale**: The factory pattern cleanly separates the "how to create an agent" concern from the daemon's session management. The factory receives enough context (session ID + cwd) to configure per-session behavior (e.g., different Rhai scripts per test scenario). Using a trait rather than a generic parameter keeps the daemon types concrete and avoids monomorphization bloat.

**Alternatives considered**:
- Generic type parameter on `Daemon<F: AgentFactory>` — infects all types with the generic, makes test utilities harder to write
- Enum dispatch (Production | Test) — violates open-closed principle, couples test code to production code
- Callback closure without trait — works but less ergonomic for complex configurations
