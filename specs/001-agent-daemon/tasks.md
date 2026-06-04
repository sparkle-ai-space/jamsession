# Tasks: Agent Daemon

**Input**: Design documents from `/specs/001-agent-daemon/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US4)
- Include exact file paths in descriptions

**Testing approach**: Unit tests live in `#[cfg(test)] mod tests` inside each module. Integration tests live in `tests/integration/` and use a mock agent binary (`tests/helpers/mock_agent.rs`) that speaks minimal ACP (responds to `initialize`, `session/new`, `session/load`, echoes `prompt/start` back).

---

## Phase 1: Setup

**Purpose**: Project initialization, dependencies, and basic structure

- [x] T001 Add primary dependencies (agent-client-protocol, acpr, tokio, serde, serde_json, tokio-util, nix, uuid, chrono) via `cargo add` in Cargo.toml
- [x] T002 Create module structure: `src/daemon.rs`, `src/session.rs`, `src/agent.rs`, `src/bridge.rs`, `src/state.rs`, `src/error.rs`
- [x] T003 [P] Define error types (daemon errors, ACP errors) in `src/error.rs`
- [x] T004 [P] Implement CLI arg parsing (subcommands: `daemon`, `acp`) in `src/main.rs`

**Validate**: `cargo build` succeeds, `cargo run -- --help` shows subcommands.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that ALL user stories depend on

**CRITICAL**: No user story work can begin until this phase is complete

- [x] T005 Implement `DaemonState` and `SessionRecord` structs with serde serialization in `src/state.rs`
- [x] T006 Implement state file persistence (atomic write via temp+rename, load on startup) in `src/state.rs`
- [x] T007 Implement `CachedCapabilities` struct and lookup logic in `src/state.rs`
- [x] T008 Implement agent spawning using `Acpr::new("claude-acp")` + `Client.builder().connect_with(...)` in `src/agent.rs`
- [x] T009 Implement `BridgeHandler` (dynamic handler that forwards all dispatches to agent connection) in `src/bridge.rs`
- [x] T010 Implement Unix socket listener and per-client task spawning in `src/daemon.rs`
- [x] T011 Implement `Agent.builder()` connection setup with static handlers for `initialize` and `session/list` in `src/daemon.rs`
- [x] T012 Write `src/guidelines.md` with initial agent interaction guidelines (included via `include_str!`)

**Tests to write and pass**:

Unit tests in `src/state.rs`:
- Serialize/deserialize `DaemonState` round-trip (empty state, state with sessions)
- Atomic write: write state, read it back, verify contents
- Load from missing file returns empty state
- Load from corrupt file returns empty state (graceful degradation)
- `CachedCapabilities` lookup: match hit, miss on different capabilities

Integration test `tests/integration/daemon_startup.rs`:
- Start daemon, verify socket file is created at expected path
- Connect to socket, send `initialize`, verify capabilities response
- Send `session/list` on empty state, verify empty list returned
- Shut down daemon, verify socket file is cleaned up

**Checkpoint**: Daemon can start, listen on socket, answer `initialize` (from cache) and `session/list` (from state file). Agent spawning and bridging infrastructure ready.

---

## Phase 3: User Story 1 — Connect and Start/Resume a Session (Priority: P1) MVP

**Goal**: Clients can connect, create new sessions, and resume existing ones with full history replay. This is the core interaction model.

**Independent Test**: Start daemon, connect client, create session, disconnect, reconnect with `session/load`, verify history is replayed. Also test `session/resume` (no replay) and one-client-per-session enforcement.

### Implementation for User Story 1

- [x] T013 [US1] Implement `session/new` handler: spawn agent via `cx.spawn()`, ACP init, send `session/new` to agent with MCP server declaration, install `BridgeHandler`, respond to client in `src/daemon.rs`
- [x] T014 [US1] Implement `session/load` handler (agent dead): spawn agent, send `session/load`, relay history replay notifications to client, install bridge in `src/daemon.rs`
- [x] T015 [US1] Implement `session/load` handler (agent alive): replay in-memory buffer to client, install bridge in `src/daemon.rs`
- [x] T016 [US1] Implement `session/resume` handler (agent dead): spawn agent, send `session/load` (buffer replay but don't relay to client), install bridge in `src/daemon.rs`
- [x] T017 [US1] Implement `session/resume` handler (agent alive): install bridge immediately (no replay) in `src/daemon.rs`
- [x] T018 [US1] Implement in-memory message buffer: record all ACP messages on agent stdio pipe in `src/session.rs`
- [x] T019 [US1] Implement one-client-per-session enforcement: disconnect existing client on new `session/load`/`session/resume` in `src/session.rs`
- [x] T020 [US1] Implement interaction guidelines delivery: send `prompt/start` with compiled guidelines as first prompt after session setup in `src/session.rs`
- [x] T021 [US1] Implement capabilities cache population: on cache miss, spawn temp agent, forward `initialize`, cache response, kill temp agent in `src/agent.rs`
- [x] T022 [US1] Implement `acp` mode: stdio-based ACP client that connects to daemon socket (auto-starting daemon if socket missing) in `src/main.rs`

**Tests to write and pass**:

Integration tests in `tests/integration/session_lifecycle.rs`:
- **New session**: connect, `initialize`, `session/new` → verify session ID returned, verify `prompt/start` (guidelines) is delivered to agent
- **Load session (agent dead)**: create session, kill agent, `session/load` → verify history replay notifications arrive before response
- **Load session (agent alive)**: create session, send a prompt, connect second client, `session/load` → verify in-memory buffer is replayed
- **Resume session (agent dead)**: `session/resume` → verify NO replay to client, but agent receives `session/load` internally
- **Resume session (agent alive)**: `session/resume` → verify immediate bridge, no replay
- **One client per session**: two clients both do `session/load` on same session → first client gets disconnected
- **Capabilities caching**: first `initialize` spawns temp agent; second `initialize` with same capabilities returns cached response without spawning

Integration test `tests/integration/acp_mode.rs`:
- Run binary in `acp` mode when daemon isn't running → verify daemon is auto-started
- Run binary in `acp` mode when daemon is running → verify connects directly

**Checkpoint**: Full session lifecycle works — create, load (dead/alive), resume (dead/alive), single-client enforcement, capabilities caching.

---

## Phase 4: User Story 4 — Agent Process Lifecycle (Priority: P2)

**Goal**: Agent processes are ephemeral — spun down when idle, respawned on demand. Resource efficient across many sessions.

**Independent Test**: Start a session, let agent go idle, verify process is killed after timeout, send new input, verify agent resumes with full context.

### Implementation for User Story 4

- [x] T023 [US4] Implement `LifecycleState` enum and state machine transitions in `src/session.rs`
- [x] T024 [US4] Implement turn completion detection: track `prompt/start` response as turn-complete signal in `src/session.rs`
- [x] T025 [US4] Implement pipe quiescence detection: 10-second silence timer after turn completion in `src/session.rs`
- [x] T026 [US4] Implement idle timer: start after quiescence when no clients connected, kill agent on expiry in `src/session.rs`
- [x] T027 [US4] Implement client-keeps-alive logic: connected client prevents idle timer from starting in `src/session.rs`
- [x] T028 [US4] Implement agent kill sequence: SIGTERM then SIGKILL, discard in-memory buffer, transition to `AgentDead` in `src/agent.rs`
- [x] T029 [US4] Implement working directory monitoring: detect deleted cwd, terminate agent, remove session from state in `src/session.rs`
- [x] T030 [US4] Implement daemon always sends `session/load` to agent (not `session/resume`): ensures buffer population for future clients in `src/agent.rs`

**Tests to write and pass**:

Unit tests in `src/session.rs`:
- `LifecycleState` transitions: verify correct state for each input (message → Active, turn complete → TurnComplete, quiescence → Quiescent, timeout → kill)
- Verify invalid transitions are rejected (e.g., can't go from `AgentDead` to `TurnComplete`)
- Verify client connection resets state to `Active`

Integration test `tests/integration/agent_lifecycle.rs`:
- **Idle spin-down**: create session, disconnect client, wait for quiescence + idle timeout → verify agent process is killed
- **Client keeps alive**: create session, keep client connected, wait past idle timeout → verify agent is NOT killed
- **Respawn on demand**: spin down agent, then `session/load` → verify agent is respawned and history replayed
- **Directory deleted**: create session, delete working directory → verify agent killed and session removed from state
- **Buffer discarded on death**: create session, send prompts (populates buffer), kill agent → verify buffer is empty after respawn begins

**Checkpoint**: Agents are spun down after idle timeout, respawned on demand with full context. Deleted directories trigger cleanup.

---

## Phase 5: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [x] T031 [P] Implement structured logging (tracing) across all modules in `src/main.rs` and module files
- [x] T032 [P] Implement ACP error responses: meaningful errors for agent spawn failure, session not found, etc. in `src/error.rs`
- [x] T033 Implement graceful daemon shutdown: stop all agents, clean up socket file in `src/daemon.rs`
- [x] T034 Implement daemon auto-start logic in `acp` mode: if socket doesn't exist, spawn daemon process, retry connect in `src/main.rs`
- [x] T035 [P] Create `tests/helpers/mock_agent.rs`: minimal ACP-speaking binary for integration tests

**Final validation**:
- `cargo test --all` passes (all unit + integration tests)
- `cargo clippy --all --workspace` has no warnings
- Manual walkthrough of quickstart.md scenarios
- Graceful shutdown: start daemon, create sessions, send SIGTERM → verify agents killed, socket removed, no panics

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational — this is the MVP
- **User Story 4 (Phase 4)**: Depends on Foundational; integrates with US1 (session/agent lifecycle)
- **Polish (Phase 5)**: Depends on all user stories being complete

### User Story Dependencies

- **US1 (P1)**: No dependencies on other stories — the MVP
- **US4 (P2)**: Uses session/agent infrastructure from US1 for lifecycle management

### Within Each User Story

- Models/types before services/logic
- Core implementation before integration with daemon
- Each story independently testable at its checkpoint

### Parallel Opportunities

- T003, T004 (setup) can run in parallel
- T031, T032, T035 (polish) can run in parallel
- Once Foundational completes, US4 implementation can proceed in parallel with US1 if desired (though US1 first is recommended for MVP)

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks all stories)
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: Connect, create session, disconnect, reconnect, verify replay
5. Deploy/demo if ready

### Incremental Delivery

1. Setup + Foundational → Infrastructure ready
2. Add US1 → Session lifecycle works → MVP!
3. Add US4 → Agents spin down/up efficiently (pairs naturally with US1)
4. Polish → Logging, error handling, tests

### Recommended Order

US1 → US4 (idle management is essential before adding more features that keep spawning agents)

---

## Notes

- [P] tasks = different files, no dependencies on incomplete tasks
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable at its checkpoint
- Commit after each task or logical group
- The `agent-client-protocol` crate provides connection building, message types, and transport — lean on it heavily
- The `acpr` crate (v0.4+) provides agent resolution and launching
- GitHub PR integration is specified separately in `specs/002-github-integration/`
