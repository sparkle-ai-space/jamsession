# Design and implementation

This section documents the internal architecture of Jamsession for contributors and anyone curious about how the daemon works.

## How to read these docs

Start with this page for the high-level architecture, then walk through the flow pages to see how each operation moves through the code:

- **[New session](./flow-new-session.md)** — connect, initialize, create session, bridge
- **[Reconnect](./flow-reconnect.md)** — load (dead/alive agent) and resume
- **[Message bridge](./flow-message-bridge.md)** — steady-state bidirectional routing
- **[Idle spin-down](./flow-idle-spindown.md)** — quiescence + idle timeout → agent kill
- **[Agent crash](./flow-agent-crash.md)** — detection and recovery
- **[CWD health check](./flow-cwd-health.md)** — periodic cleanup of deleted directories

Each page includes anchor code references that link directly to the source.

## Design principles

**Single writer**: The dispatcher is the sole owner of `sessions: HashMap<SessionId, Session>`. No mutexes needed for session state.

**Inputs vs events**: `DispatcherMessage` is what the dispatcher processes; `LifecycleEvent` is what it emits. Tests subscribe to the event channel and assert on outcomes. The two enums are cleanly separated — no variant appears in both.

**Typed dispatch via `MatchDispatch`**: The dispatcher uses `MatchDispatch` to route incoming `Dispatch` from clients to typed request handlers (`InitializeRequest`, `NewSessionRequest`, etc.), responding via `Responder<T>` directly. No oneshot request-reply channels.

**Fire-and-forget for events**: Messages like `ClientDisconnected`, `AgentDisconnected`, and timer expirations don't need replies — the dispatcher handles them unilaterally.

**Timers as messages**: Instead of spawning timer tasks that grab mutexes, the dispatcher spawns lightweight tasks that simply sleep and then send `AgentQuiescent` / `IdleTimeoutElapsed` back to the dispatcher channel. The dispatcher checks whether the timer is still relevant (client may have reconnected) before acting.

**Agent pipe isolation**: Each live agent has a dedicated task (`agent_pipe`) spawned by the dispatcher. The task reads from the agent's transport and forwards dispatches as `FromAgent { agent_id, dispatch }` into the dispatcher channel. The dispatcher decides what to do with each message (persist it, forward to client, update lifecycle state). If the agent process exits, the task sends `AgentDisconnected`.

## Key concepts

- **Ephemeral agents** — Agent processes are disposable. They can be killed after a turn completes. On respawn, the daemon sends `session/resume`; conversation history is owned and replayed by the daemon.

- **SQLite message store** — Session metadata and agent notifications are persisted in `jamsession.db`. Client `session/load` always replays notifications from SQLite, whether the agent is alive or needs to be respawned.

- **Multiple clients per session** — Multiple client connections can be active on a session simultaneously (`client_ids: Vec<ClientId>`). Outgoing messages from the agent are routed to the most-recently-connected client (`.last()`).

- **Generation-counter timers** — Instead of tracking and aborting timer tasks, each session has a monotonic generation counter. Timer messages carry the generation they were spawned at; stale timers are discarded on mismatch.

## Module map

| Module | File | Role |
|--------|------|------|
| `dispatcher` | `src/dispatcher.rs` | Central dispatcher: message types, session state, routing, timers, client/agent pipes |
| `daemon` | `src/daemon.rs` | Socket listener, database opening, accept loop, scope-based task management |
| `db` | `src/db.rs` | Toasty models and SQLite-backed session/message persistence |
| `agent` | `src/agent.rs` | Agent factory trait, transport creation |
| `session` | `src/session.rs` | `LifecycleEvent` enum (observable outcomes for tests/tracing) |
| `eof_signal` | `src/eof_signal.rs` | `EofSignalingTransport` wrapper for disconnect detection |
| `error` | `src/error.rs` | Error types |
| `logging` | `src/logging.rs` | Per-session log file routing via tracing layer |
