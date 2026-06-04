# Feature Specification: Integration Test Harness

**Feature Branch**: `004-integration-test-harness`

**Created**: 2026-06-03

**Status**: Draft

**Input**: User description: "Integration test harness for testing the agent daemon (spec 001), using rhaicp 2.1 for scripting both sides"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Run Integration Tests Against the Daemon (Priority: P1)

A developer runs `cargo test` and the integration test harness exercises the daemon end-to-end. Each test starts a fresh daemon instance with an isolated state directory and socket path. The daemon's agent spawn function is configured to create `RhaiAgent` instances in-process (via library API). Test clients use `RhaiClient` connected to the daemon's Unix socket. The Rust test function orchestrates one or more client connections, each driven by a Rhai script.

**Why this priority**: The harness must exist for any testing to happen.

**Independent Test**: Can be verified by running a single integration test that starts the daemon, connects a `RhaiClient` to the socket, creates a session, exchanges one prompt/response via `RhaiAgent`, and shuts down cleanly.

**Acceptance Scenarios**:

1. **Given** a test function, **When** it requests a test daemon, **Then** the harness starts the daemon on an isolated socket/state directory with an injected `RhaiAgent` factory, and the daemon is ready to accept connections within 2 seconds.
2. **Given** a running test daemon, **When** a `RhaiClient` connects to the socket and runs `list_sessions()`, **Then** it receives a valid (possibly empty) session list.
3. **Given** a connected `RhaiClient`, **When** its script calls `start_session()` and `s.prompt("say(\"hello\")")`, **Then** the `RhaiAgent` executes the prompt and the client receives "hello".
4. **Given** a completed test, **When** the test function returns (or panics), **Then** all daemon and agent processes are killed and temporary files are cleaned up.

---

### User Story 2 - Test Session Lifecycle Flows (Priority: P1)

A developer writes tests that verify the daemon's session lifecycle: creating sessions, loading dead sessions (with replay), resuming live sessions, and verifying single-client enforcement. The Rust test harness orchestrates multiple sequential `RhaiClient::execute()` calls against the same daemon socket to simulate disconnect/reconnect flows. Agent scripts use `is_load()`, `user()`, `say()`, and `receive_prompt()` to provide deterministic replay and response behavior.

**Why this priority**: Session lifecycle is the daemon's core responsibility and the primary reason for building the harness.

**Independent Test**: Can be tested by running the session lifecycle test suite and verifying each scenario passes.

**Acceptance Scenarios**:

1. **Given** a test with a first client that created a session, **When** the first `RhaiClient::execute()` completes (client disconnects) and a second `RhaiClient::execute()` calls `load_session(id)`, **Then** the daemon respawns the agent (which replays history via `user()`/`say()`) and the client receives the replay via `session.updates()`.
2. **Given** a test with a live session (agent still running), **When** a second `RhaiClient::execute()` calls `load_session(id)`, **Then** the client receives the in-memory buffer replay.
3. **Given** a test with a live session, **When** a second `RhaiClient::execute()` calls `resume_session(id)`, **Then** it is bridged immediately without replay.
4. **Given** two concurrent client connections to the same session, **When** the second client connects, **Then** the first client is disconnected (one-client-per-session enforcement).

---

### User Story 3 - Test Agent Lifecycle (Priority: P2)

A developer writes tests that verify agent spin-down behavior: the idle timer, quiescence detection, and agent respawn. The daemon is configured with short timeouts (e.g., 50ms idle timeout). Agent scripts use `sleep(ms)` to control timing and `exit(code)` to simulate crashes. The Rust test harness uses sequential `RhaiClient::execute()` calls with sleeps between them to observe lifecycle transitions.

**Why this priority**: Agent lifecycle is a core daemon feature but depends on the basic harness (P1) being functional first.

**Independent Test**: Can be tested by running a test that creates a session, lets the first client disconnect, waits for the agent to be killed, then connects a second client and verifies the agent was respawned.

**Acceptance Scenarios**:

1. **Given** a test daemon with a 50ms idle timeout, **When** the client disconnects and the agent's turn is complete, **Then** the agent process is killed after quiescence + idle timeout.
2. **Given** a test with a killed agent, **When** a new `RhaiClient::execute()` calls `load_session(id)`, **Then** a new agent is spawned and `session/load` is forwarded to it.
3. **Given** an agent script that calls `exit(1)` mid-turn, **When** the agent crashes, **Then** the daemon notifies the client and respawns the agent once.
4. **Given** an agent script that calls `sleep(500)` after responding, **When** the client disconnects, **Then** the daemon waits for quiescence (pipe silence) before starting the idle timer.

---

### Edge Cases

- What happens if the agent factory fails to create an agent? (Daemon returns an ACP error to the client; test can assert on it.)
- What happens if a test times out? (Harness applies a per-test timeout and panics with diagnostic info including ACP messages exchanged.)
- What happens if multiple tests run in parallel? (Each test uses its own isolated socket and state directory — no conflicts.)
- What happens if a test panics before cleanup? (Drop guards ensure processes are killed and temp dirs removed.)

## Requirements *(mandatory)*

### Functional Requirements

**Test Daemon Setup**

- **FR-001**: System MUST provide a Rust test helper that starts a daemon instance with an isolated temporary directory for state and socket files.
- **FR-002**: The test harness MUST configure the daemon's agent spawn factory to create `RhaiAgent` instances in-process (configured via the rhaicp library API).
- **FR-003**: System MUST allow configuring the daemon's idle timeout to short values (e.g., 50ms) for fast lifecycle tests.
- **FR-004**: System MUST clean up all daemon processes, agent processes, and temporary files when a test completes (including on panic via Drop guards).
- **FR-005**: System MUST support running multiple integration tests in parallel without interference (isolated sockets and state).

**Client Connections**

- **FR-006**: System MUST connect `RhaiClient` to the daemon's Unix socket via byte streams (`ConnectTo<Client>`).
- **FR-007**: System MUST support multiple sequential `RhaiClient::execute()` calls against the same daemon socket within a single test (simulating disconnect/reconnect).
- **FR-008**: The Rust test harness MUST be responsible for timing between connections (e.g., sleeping between disconnects and reconnects for lifecycle tests).

**Agent Configuration**

- **FR-009**: The daemon MUST allow its agent spawn function to be configured (e.g., via a trait or closure). The factory receives session context (at minimum session ID and cwd) so it can return a differently-configured agent per spawn. In production, it spawns a child process. In tests, the harness injects a factory that creates `RhaiAgent` instances in-process.
- **FR-010**: The test harness MUST configure `RhaiAgent` via the library API (`RhaiAgent::new().prior_sessions(...).new_session_script(...)`) — no CLI binary or temp script files needed.
- **FR-011**: Agent scripts MUST be able to use `sleep(ms)` for timing control and `exit(code)` for crash simulation. When running in-process, `exit(code)` MUST panic (rather than calling `std::process::exit()`) so the harness can catch it and treat it as an agent crash. This requires a rhaicp extension (configurable exit behavior).

**Assertions & Diagnostics**

- **FR-012**: System MUST produce clear failure messages when tests fail, including the ACP messages exchanged.
- **FR-013**: System SHOULD provide convenience assertion helpers for common patterns (e.g., "replay contained N updates", "session list has N entries").

### Key Entities

- **TestDaemon**: A Rust struct that starts and manages an isolated daemon instance. Owns the temp directory, socket path, and agent binary configuration. Implements `Drop` for cleanup.
- **Agent Scripts**: Rhai script strings passed to `RhaiAgent` via its builder API. Use `receive_prompt()`, `say()`, `user()`, `is_load()`, `sleep()`, `exit()`.
- **Client Scripts**: Rhai script strings passed to `RhaiClient::execute()`. Use `start_session()`, `load_session()`, `resume_session()`, `list_sessions()`, `session.prompt()`, `session.updates()`, `session.session_id()`, `sleep()`.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A new integration test covering a basic session lifecycle (connect, create session, send prompt, receive response) can be written in under 20 lines of Rust + Rhai.
- **SC-002**: The full integration test suite runs in under 10 seconds (using short timeouts for lifecycle tests).
- **SC-003**: Tests can run in parallel (`cargo test` default) without flakiness or interference.
- **SC-004**: When a test fails, the error output includes enough context to diagnose the issue without adding manual debug logging.

## Clarifications

### Session 2026-06-04

- Q: How should crash simulation (`exit(code)`) work given in-process agents? → A: Replace `exit()` with a panic that the harness catches as "agent crashed" (requires rhaicp change)
- Q: Does the agent factory receive session context (ID, cwd) to configure per-session behavior? → A: Yes, factory receives session ID + cwd and returns a configured agent per spawn

## Assumptions

- The daemon code (spec 001) is the system under test; the harness tests it as a black box via the Unix socket.
- `rhaicp` 2.1 is used as a library dependency. One extension is needed: `exit(code)` must be configurable to panic (instead of `std::process::exit()`) when running in-process, so the harness can catch it as a simulated crash.
- The daemon's agent spawn function is made configurable (trait or closure), allowing tests to inject `RhaiAgent` instances in-process rather than spawning a binary.
- Integration tests are Rust tests in the `tests/` directory, using `#[tokio::test]`.
- Test isolation relies on unique temporary directories (via `tempfile` crate or similar).
- The Rust test harness (not Rhai scripts) is responsible for orchestrating multiple connections and managing timing between them.
