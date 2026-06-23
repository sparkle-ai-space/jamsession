# Design and implementation

This section documents the internal architecture of Jamsession for contributors and anyone curious about how the daemon works.

## How to read these docs

Start with the **[daemon actor architecture](./daemon_actor.md)** — it explains the central pattern (single actor task owns all state, everything else communicates via messages) and includes the full message type definitions pulled live from the source.

Then walk through the flow pages to see how each operation moves through the code:

- **[New session](./flow-new-session.md)** — connect, initialize, create session, bridge
- **[Reconnect](./flow-reconnect.md)** — load (dead/alive agent) and resume
- **[Message bridge](./flow-message-bridge.md)** — steady-state bidirectional routing
- **[Idle spin-down](./flow-idle-spindown.md)** — quiescence + idle timeout → agent kill
- **[Agent crash](./flow-agent-crash.md)** — detection and recovery
- **[CWD health check](./flow-cwd-health.md)** — periodic cleanup of deleted directories

Each page includes `{anchor}` code references that link directly to the source.

## Key concepts

- **Ephemeral agents** — Agent processes are disposable. They can be killed after a turn completes. On respawn, the daemon sends `session/load` and the agent reconstructs state from its own store. The daemon never owns conversation history.

- **In-memory buffer** — While an agent is alive, the actor records all notifications flowing through the bridge. This serves `session/load` from late-joining clients when the agent is already running — the actor replays the buffer instead of asking the agent to replay.

- **One client per session** — Only one client connection can be active on a session at a time. A second client supersedes the first. This simplifies routing (no fan-out) and matches the expected editor workflow.

- **Generation-counter timers** — Instead of tracking and aborting timer tasks, each session has a monotonic generation counter. Timer messages carry the generation they were spawned at; stale timers are discarded on mismatch.

## Module map

| Module | File | Role |
|--------|------|------|
| `actor` | `src/actor.rs` | Central actor: message types, session state, routing, timers |
| `daemon` | `src/daemon.rs` | Socket listener, per-client ACP connection, request handlers |
| `agent` | `src/agent.rs` | Agent factory trait, spawn, ACP init handshake, capabilities probe |
| `session` | `src/session.rs` | `LifecycleEvent` enum (observable outcomes for tests/tracing) |
| `state` | `src/state.rs` | Persistent state file (session registry, capabilities cache) |
| `error` | `src/error.rs` | Error types |
| `logging` | `src/logging.rs` | Per-session log file routing via tracing layer |
