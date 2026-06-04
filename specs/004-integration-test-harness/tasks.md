# Tasks: Integration Test Harness

**Input**: Design documents from `/specs/004-integration-test-harness/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

---

## Phase 1: Setup

**Purpose**: Add rhaicp dependency and prepare project structure for the test harness

- [ ] T001 Add `rhaicp` 2.1 as a dev-dependency (with path override) and `trait-variant` as a dependency in Cargo.toml
- [ ] T002 Create test harness directory structure: tests/harness/mod.rs, tests/harness/transport.rs

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Extract AgentFactory trait from the daemon and wire it through — MUST complete before any harness-based tests can run

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T003 Define `AgentFactory` trait and `AcprFactory` production impl in src/agent.rs per contracts/agent-factory.md
- [ ] T004 Add `Arc<dyn AgentFactory>` field to `Daemon` struct and thread it through `new_with_paths` / `new_with_factory` constructors in src/daemon.rs
- [ ] T005 Wire the factory into `SessionManager` so `handle_new_session` and `handle_load_session` call `factory.spawn_agent()` instead of `AgentManager::spawn_for_session()` in src/session.rs
- [ ] T006 Add `panic(msg)` function to rhaicp's `register_common_functions` and remove `exit(code)` in the rhaicp crate (external: /local/home/nikomat/dev/rhaicp/src/lib.rs)
- [ ] T007 Implement `UnixSocketTransport` (implements `ConnectTo<Client>` by connecting to a Unix socket and wrapping in `ByteStreams`) in tests/harness/transport.rs
- [ ] T008 Implement `TestDaemon`, `TestDaemonConfig`, and `RhaiAgentFactory` in tests/harness/mod.rs per contracts/test-daemon-api.md
- [ ] T009 Verify foundation: write a minimal smoke test in tests/daemon_startup.rs that uses `TestDaemon::start()` + `execute_client()` to call `list_sessions()` and get an empty array

**Checkpoint**: Foundation ready — TestDaemon starts, connects RhaiClient, and can exchange ACP messages through the daemon with an in-process RhaiAgent

---

## Phase 3: User Story 1 — Run Integration Tests Against the Daemon (Priority: P1) 🎯 MVP

**Goal**: A developer can write integration tests that start a daemon, connect a RhaiClient, create a session, exchange prompts/responses via RhaiAgent, and shut down cleanly.

**Independent Test**: Run `cargo test --test session_lifecycle basic_session` and verify the daemon accepts a connection, creates a session, processes a prompt through the RhaiAgent, and returns the response.

### Implementation for User Story 1

- [ ] T010 [US1] Write test `basic_session_prompt_response` in tests/session_lifecycle.rs: start daemon with echo agent script, start_session, prompt, assert response
- [ ] T011 [US1] Write test `list_sessions_shows_created_session` in tests/session_lifecycle.rs: start_session then list_sessions, assert session appears
- [ ] T012 [US1] Write test `multiple_sessions_independent` in tests/session_lifecycle.rs: create two sessions with different scripts, prompt each, assert independent responses
- [ ] T013 [US1] Write test `agent_factory_failure_returns_error` in tests/session_lifecycle.rs: configure factory to return an error, attempt start_session, assert ACP error response
- [ ] T014 [US1] Write test `test_timeout_produces_diagnostic` in tests/session_lifecycle.rs: agent script sleeps forever, client prompts with a short timeout wrapper, assert panic with useful message

**Checkpoint**: User Story 1 complete — basic harness usage verified end-to-end

---

## Phase 4: User Story 2 — Test Session Lifecycle Flows (Priority: P1)

**Goal**: A developer can write tests verifying session lifecycle: create, load (with replay), resume (live bridging), and single-client enforcement.

**Independent Test**: Run `cargo test --test session_lifecycle load_` and verify load replays history, resume bridges immediately, and second client disconnects the first.

### Implementation for User Story 2

- [ ] T015 [US2] Write test `load_session_replays_history` in tests/session_lifecycle.rs: first client creates session and prompts, second client loads and verifies replay updates via `s.updates()`
- [ ] T016 [US2] Write test `load_dead_session_respawns_agent` in tests/session_lifecycle.rs: first client creates session, disconnect, wait for agent death, second client loads session and verifies new agent spawned with replay
- [ ] T017 [US2] Write test `resume_live_session_bridges_immediately` in tests/session_lifecycle.rs: first client creates session (agent still alive), second client resumes and can prompt without replay
- [ ] T018 [US2] Write test `single_client_enforcement` in tests/session_lifecycle.rs: two clients connect to same session, verify first is disconnected when second arrives
- [ ] T019 [US2] Write test `sequential_disconnect_reconnect` in tests/session_lifecycle.rs: multiple sequential execute_client calls simulating disconnect/reconnect patterns

**Checkpoint**: User Story 2 complete — full session lifecycle coverage

---

## Phase 5: User Story 3 — Test Agent Lifecycle (Priority: P2)

**Goal**: A developer can write tests verifying agent spin-down (idle timer, quiescence) and respawn after crash.

**Independent Test**: Run `cargo test --test agent_lifecycle` and verify agent is killed after idle timeout and respawned on next load.

### Implementation for User Story 3

- [ ] T020 [US3] Write test `agent_killed_after_idle_timeout` in tests/agent_lifecycle.rs: configure 50ms idle timeout, create session, disconnect, sleep 200ms, verify agent is dead (load triggers respawn)
- [ ] T021 [US3] Write test `agent_respawn_after_crash` in tests/agent_lifecycle.rs: agent script calls `panic("crash")`, verify daemon handles it gracefully, next load respawns agent
- [ ] T022 [US3] Write test `quiescence_detection_before_idle` in tests/agent_lifecycle.rs: agent script does `sleep(500)` after responding, verify agent is NOT killed during sleep (quiescence not yet reached)
- [ ] T023 [US3] Write test `client_reconnect_cancels_idle_timer` in tests/agent_lifecycle.rs: create session, disconnect, before idle expires reconnect, verify agent stays alive

**Checkpoint**: User Story 3 complete — agent lifecycle fully tested

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Cleanup existing tests, edge cases, and parallel safety validation

- [ ] T024 [P] Migrate existing tests in tests/daemon_startup.rs to use the TestDaemon harness (remove duplicated `start_daemon` helper)
- [ ] T025 [P] Migrate existing tests in tests/session_lifecycle.rs raw-socket tests to use the TestDaemon harness
- [ ] T026 Run `cargo test` with default parallelism and verify no flakiness across all integration tests
- [ ] T027 Validate quickstart.md examples compile and pass by running them as tests

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Stories (Phase 3-5)**: All depend on Foundational phase completion
  - US1 and US2 can proceed in parallel (both P1, different test scenarios)
  - US3 depends on US1 being functional (needs basic harness working)
- **Polish (Phase 6)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) — No dependencies on other stories
- **User Story 2 (P1)**: Can start after Foundational (Phase 2) — Independent of US1 (different test scenarios)
- **User Story 3 (P2)**: Can start after Foundational (Phase 2) — Independent but benefits from US1 confirming the harness works

### Within Each User Story

- Tests ARE the implementation (this feature is a test harness)
- Each test file is independent
- Tests within a file can run in any order

### Parallel Opportunities

- T001 and T002 can run in parallel (different files)
- T003, T006, T007 can run in parallel (different files/crates)
- All US1 tests (T010-T014) can run in parallel (same file but independent test functions)
- All US2 tests (T015-T019) can run in parallel
- All US3 tests (T020-T023) can run in parallel
- T024 and T025 can run in parallel (different test files)

---

## Parallel Example: Foundational Phase

```bash
# These can run in parallel (different files):
Task T003: "Define AgentFactory trait in src/agent.rs"
Task T006: "Add panic() function in rhaicp"
Task T007: "Implement UnixSocketTransport in tests/harness/transport.rs"

# Then sequentially:
Task T004: "Wire factory into Daemon (depends on T003)"
Task T005: "Wire factory into SessionManager (depends on T004)"
Task T008: "Implement TestDaemon (depends on T004, T007)"
Task T009: "Smoke test (depends on T008)"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (Cargo.toml + directory structure)
2. Complete Phase 2: Foundational (AgentFactory trait + TestDaemon harness)
3. Complete Phase 3: User Story 1 (basic session tests)
4. **STOP and VALIDATE**: `cargo test --test session_lifecycle` passes
5. Harness is usable for further test development

### Incremental Delivery

1. Complete Setup + Foundational → TestDaemon working
2. Add User Story 1 → Basic prompt/response verified (MVP!)
3. Add User Story 2 → Session lifecycle (load/resume/reconnect) verified
4. Add User Story 3 → Agent lifecycle (idle/crash/respawn) verified
5. Polish → Existing tests migrated, no flakiness

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- This feature's "implementation" IS writing tests — the harness enables them
- The rhaicp change (T006) is in an external crate — coordinate separately
- Existing raw-socket tests continue working until migrated in Phase 6
