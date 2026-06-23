# Reconnect — load and resume

When a client reconnects to an existing session, the dispatcher decides whether to spawn a new agent (dead) or bridge to the existing one (alive). There are two client-facing operations:

- **session/load** — replays buffered history to the client
- **session/resume** — bridges immediately without replay

Both check liveness inline in the dispatcher's handler — no intermediate query message.

## Load session — agent dead

The agent was killed (idle timeout, crash, or cwd deleted). The dispatcher spawns a fresh `agent_pipe` task. The agent pipe sends `session/load` to the agent, captures replay notifications, then sends `AgentReady` back to the dispatcher.

```mermaid
sequenceDiagram
    participant C as Client
    participant CP as Client Pipe
    participant D as Dispatcher
    participant AP as Agent Pipe
    participant Ag as Agent

    C->>CP: session/load [sessionId]
    CP->>D: FromClient { client_id, dispatch }
    Note over D: MatchDispatch → handle_session_load
    Note over D: Session exists, lifecycle_state == AgentDead
    Note over D: Spawn agent_pipe task

    AP->>Ag: initialize + session/load [sessionId]
    Ag-->>AP: replay notifications
    Ag-->>AP: session/load response
    Note over AP: Captures replay via ReplayCapture handler
    AP->>D: AgentReady { agent_id, session_id, replay_notifications, ... }
    Note over D: Replay buffer to client via outgoing_tx
    D-->>CP: Responder<LoadSessionResponse>
    CP-->>C: session/load response

    Note over C,Ag: Bridged — dispatcher routes via outgoing_tx channels
```

## Load session — agent alive

The agent is still running from a previous client. No spawn needed — the dispatcher replays its in-memory notification buffer to the new client's `outgoing_tx` and wires the client to the session.

```mermaid
sequenceDiagram
    participant C as Client
    participant CP as Client Pipe
    participant D as Dispatcher
    participant Ag as Agent

    Note over Ag: Already running from previous client

    C->>CP: session/load [sessionId]
    CP->>D: FromClient { client_id, dispatch }
    Note over D: MatchDispatch → handle_session_load
    Note over D: Session exists, agent alive
    Note over D: Replay buffered notifications to client outgoing_tx
    Note over D: Add client_id to session.client_ids
    D-->>CP: Responder<LoadSessionResponse>
    CP-->>C: session/load response

    Note over C,Ag: Client bridged to live agent stream
```

## Resume session — agent dead

Same as load-dead: the dispatcher spawns an `agent_pipe` which sends `session/load` to the agent (resume always loads the agent's state). The response comes back as `AgentReady` with a `ResumeSession` responder.

## Resume session — agent alive

Same as load-alive but without replay. The client is wired to the session and picks up the live stream from the current point forward.

```mermaid
sequenceDiagram
    participant C as Client
    participant CP as Client Pipe
    participant D as Dispatcher
    participant Ag as Agent

    Note over Ag: Already running

    C->>CP: session/resume [sessionId]
    CP->>D: FromClient { client_id, dispatch }
    Note over D: MatchDispatch → handle_session_resume
    Note over D: Session exists, agent alive
    Note over D: Add client_id to session.client_ids (no replay)
    D-->>CP: Responder<ResumeSessionResponse>
    CP-->>C: session/resume response

    Note over C,Ag: Client bridged to live agent
```

## Step by step

### Dispatch

Both `session/load` and `session/resume` arrive as `FromClient` dispatches. The dispatcher's `MatchDispatch` chain routes them to `handle_session_load` or `handle_session_resume`.

```{anchor}
dispatch-session-load
```

### Implementation (load)

The load handler checks liveness inline. If the agent is dead, it spawns an `agent_pipe` task (via `self.tasks.spawn(...)`) which performs the ACP handshake, sends `session/load` to the agent, captures replay notifications, and sends `AgentReady` back. If the agent is alive, it replays the buffer and wires the client immediately.

```{anchor}
handle-session-load
```

### Dispatcher: replay buffered notifications

When `AgentReady` arrives with a `LoadSession` responder, the dispatcher replays the captured notifications to the client via `outgoing_tx.send(Dispatch::Notification(...))`.

## Integration tests

- `session_lifecycle::load_session_after_create` — load after disconnect (agent may still be alive)
- `session_lifecycle::load_nonexistent_session_returns_error` — error path
- `integration::load_live_session_replays_buffer` — load with alive agent, verify replay
- `integration::resume_live_session_bridges_immediately` — resume and prompt immediately
- `integration::load_dead_session_respawns_agent` *(ignored — requires independent agent connections)*
