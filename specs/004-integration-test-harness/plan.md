# Implementation Plan: Integration Test Harness

**Branch**: `004-integration-test-harness` | **Date**: 2026-06-04 | **Spec**: `specs/004-integration-test-harness/spec.md`

**Input**: Feature specification from `/specs/004-integration-test-harness/spec.md`

## Summary

An integration test harness for the agent daemon (spec 001) that uses rhaicp 2.1 for scripting both the agent and client sides. The harness provides a `TestDaemon` utility that starts isolated daemon instances with injected in-process `RhaiAgent` factories, enabling end-to-end testing of session lifecycle, agent lifecycle, and ACP message exchange without spawning external processes.

## Technical Context

**Language/Version**: Rust 2024 edition (stable)

**Primary Dependencies**:
- `agent-client-protocol` 0.13.1 — ACP types, connection builders, transport
- `rhaicp` 2.1 — `RhaiAgent` (in-process agent) and `RhaiClient` (scripted client)
- `tokio` — async runtime, Unix sockets, timers
- `tempfile` — isolated temp directories per test
- `tokio-util` (compat) — adapting tokio streams for ACP transports

**Storage**: N/A (tests use ephemeral temp directories)

**Testing**: `cargo test` with `#[tokio::test]` — this feature IS the test infrastructure

**Target Platform**: Linux

**Project Type**: Test harness (library code in `src/` for the factory trait, test utilities in `tests/`)

**Performance Goals**: Full test suite under 10 seconds

**Constraints**: Tests must run in parallel without interference; each test fully isolated

**Scale/Scope**: ~10-20 integration tests covering session lifecycle, agent lifecycle, and edge cases

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Constitution is a blank template — no project-specific gates or constraints defined. Proceeding without gate violations.

**Post-design re-check**: No violations. The harness adds a trait abstraction (`AgentFactory`) to the daemon — this is the minimal change needed to enable testability and does not introduce unnecessary complexity.

## Project Structure

### Documentation (this feature)

```text
specs/004-integration-test-harness/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   ├── agent-factory.md # AgentFactory trait contract
│   └── test-daemon-api.md # TestDaemon public API contract
└── tasks.md             # Phase 2 output (/speckit-tasks command)
```

### Source Code (repository root)

```text
src/
├── agent.rs             # Modified: extract AgentFactory trait, AcprFactory impl
├── daemon.rs            # Modified: accept Arc<dyn AgentFactory>, pass to session manager
├── session.rs           # Modified: use factory for agent spawning
└── ...                  # Other files unchanged

tests/
├── harness/
│   ├── mod.rs           # TestDaemon, TestDaemonConfig, RhaiAgentFactory
│   └── transport.rs     # UnixSocketTransport (ConnectTo<Client> adapter)
├── session_lifecycle.rs # P1: session create/load/resume/single-client enforcement
├── agent_lifecycle.rs   # P2: idle timeout, quiescence, respawn, crash handling
└── daemon_startup.rs    # Existing: basic socket + initialize tests (updated to use harness)
```

**Structure Decision**: The `AgentFactory` trait lives in `src/agent.rs` alongside the existing agent code. Test harness utilities live in `tests/harness/` (shared via `mod harness;` in each test file). Integration tests are top-level files in `tests/`.

## Complexity Tracking

No constitution violations to justify.
