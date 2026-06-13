# Key sequence diagrams

## Fresh connection -- new session

```mermaid
sequenceDiagram
    participant C as Client
    participant CT as Client Task
    participant A as Actor
    participant Ag as Agent

    C->>CT: connect [Unix socket]
    CT->>A: Initialize { req, reply }
    Note over A: Cache hit or spawn temp agent
    A-->>CT: capabilities
    CT-->>C: capabilities

    CT->>A: ListSessions { req, reply }
    A-->>CT: sessions [from state file]
    CT-->>C: sessions

    C->>CT: session/new [dir: /project]
    Note over CT: Runs in cx.spawn() — block_task() safe
    CT->>Ag: spawn + initialize
    CT->>Ag: session/new [mcpServers]
    Ag-->>CT: session created [sessionId]
    Note over CT: Install ClientForwarder + AgentForwarder
    CT->>A: SessionCreated { session_id, cwd, client_cx, agent_cx }
    Note over A: Persist to state file, store session
    CT-->>C: session created

    Note over C,Ag: Bridged — client ↔ forwarder ↔ actor ↔ forwarder ↔ agent
```

Source locations:
- **accept loop** — {anchor}`accept-loop`
- **initialize (dispatch)** — {anchor}`handle-initialize`
- **session/list (dispatch)** — {anchor}`handle-session-list`
- **session/new (dispatch)** — {anchor}`dispatch-session-new`
- **session/new (impl)** — {anchor}`handle-session-new`
- **message routing** — {anchor}`route-messages`

Integration tests:
- `daemon_startup::daemon_creates_socket_file` — connect step (daemon listens on Unix socket)
- `daemon_startup::daemon_accepts_connection_and_responds_to_initialize` — initialize exchange
- `daemon_startup::session_list_returns_empty` — session/list on fresh daemon
- `session_lifecycle::new_session_creates_session_and_returns_id` — full session/new flow through agent
- `session_lifecycle::new_session_persists_to_state_file` — state file persistence after session/new
- `session_lifecycle::session_list_shows_created_session` — session/list after session/new
- `session_lifecycle::new_session_with_invalid_cwd_returns_error` — error path (invalid directory)
- `integration::basic_session_prompt_response` — end-to-end prompt through actor routing
- `integration::multiple_sessions_independent` — two sessions with independent agents

## Reconnect -- load session (agent dead)

```mermaid
sequenceDiagram
    participant C as Client
    participant CT as Client Task
    participant A as Actor
    participant Ag as Agent

    C->>CT: session/load [sessionId]
    CT->>A: QuerySessionState { session_id, reply }
    A-->>CT: { agent_dead: true, cwd }
    Note over CT: Spawns new agent in cx.spawn()
    CT->>Ag: spawn + initialize
    CT->>Ag: session/load [sessionId]
    Ag-->>CT: session/update [history replay]
    CT-->>C: session/update [forwarded]
    Ag-->>CT: session/load response
    Note over CT: Install forwarders
    CT->>A: SessionReconnected { session_id, client_cx, agent_cx }
    CT-->>C: session/load response

    Note over C,Ag: Bridged — daemon routes via actor
```

Source locations:
- **session/load (dispatch)** — {anchor}`dispatch-session-load`
- **session/load (impl, dead branch)** — {anchor}`handle-session-load`

Integration tests:
- `session_lifecycle::load_session_after_create` — create session, drop connection, load on new connection
- `session_lifecycle::load_nonexistent_session_returns_error` — error path (unknown sessionId)
- `integration::load_dead_session_respawns_agent` *(ignored — requires independent agent connections)*

## Reconnect -- load session (agent alive)

```mermaid
sequenceDiagram
    participant C as Client
    participant CT as Client Task
    participant A as Actor
    participant Ag as Agent

    Note over Ag: Already running from previous client

    C->>CT: session/load [sessionId]
    CT->>A: QuerySessionState { session_id, reply }
    A-->>CT: { agent_dead: false, cwd }
    Note over CT: No agent spawn needed
    Note over CT: Install ClientForwarder
    CT->>A: SessionReconnected { session_id, client_cx, replay_to_client: true }
    Note over A: Replay buffered notifications to new client
    CT-->>C: session/load response

    Note over C,Ag: Client bridged to live agent stream
```

Source locations:
- **session/load (impl, alive branch)** — {anchor}`handle-session-load`
- **replay in actor** — see `handle_session_reconnected` in `actor.rs`

Integration tests:
- `integration::load_live_session_replays_buffer` — load with agent alive, verify buffered messages replay

## Reconnect -- resume session (agent alive)

```mermaid
sequenceDiagram
    participant C as Client
    participant CT as Client Task
    participant A as Actor
    participant Ag as Agent

    Note over Ag: Already running

    C->>CT: session/resume [sessionId]
    CT->>A: QuerySessionState { session_id, reply }
    A-->>CT: { agent_dead: false, cwd }
    Note over CT: Install ClientForwarder (no replay)
    CT->>A: SessionReconnected { session_id, client_cx, replay_to_client: false }
    CT-->>C: session/resume response

    Note over C,Ag: Client bridged to live agent
```

Integration tests:
- `integration::resume_live_session_bridges_immediately` — resume and prompt immediately on live agent

## Idle spin-down

```mermaid
sequenceDiagram
    participant CT as Client Task
    participant A as Actor
    participant T as Timer

    Note over CT: TCP/socket closes (connect_to returns)
    CT->>A: ClientDisconnected { session_id }
    Note over A: Drop client_cx, bump generation, spawn quiescence timer

    T->>A: AgentQuiescent { session_id, generation }
    Note over A: Check generation matches (stale → discard)
    Note over A: Transition to Quiescent, spawn idle timer

    T->>A: IdleTimeoutElapsed { session_id, generation }
    Note over A: Check generation matches
    Note over A: Kill agent (drop agent_cx), clear buffer
```

Source locations:
- **client disconnect (send)** — {anchor}`client-disconnect`
- **quiescence + idle timer (actor)** — {anchor}`disconnect-and-idle`

Integration tests:
- `integration::agent_killed_after_idle_timeout` *(ignored — requires independent agent connections)*

## Agent crash detection

```mermaid
sequenceDiagram
    participant Ag as Agent
    participant A as Actor

    Note over Ag: Process exits unexpectedly
    Ag--xA: AgentExited { session_id }
    Note over A: Mark session AgentDead
    Note over A: Clear buffer
```

Source locations:
- **agent exit handling** — {anchor}`handle-agent-exited`

Integration tests: *none yet*

## Message flow through the bridge

During normal operation, the actor routes messages bidirectionally via forwarders:

```mermaid
sequenceDiagram
    participant C as Client
    participant CF as ClientForwarder
    participant A as Actor
    participant AF as AgentForwarder
    participant Ag as Agent

    C->>CF: prompt/start [Dispatch]
    CF->>A: ClientMessage { session_id, dispatch }
    A->>Ag: send_proxied_message(dispatch)

    Ag->>AF: session/update [Dispatch]
    AF->>A: AgentMessage { session_id, dispatch }
    Note over A: Buffer notification, bump generation
    A->>C: send_proxied_message(dispatch)

    Ag->>AF: prompt/end [Dispatch]
    AF->>A: AgentMessage { session_id, dispatch }
    A->>C: send_proxied_message(dispatch)
```

Source locations:
- **message routing** — {anchor}`route-messages`
- **forwarder handlers** — see `ClientForwarder` / `AgentForwarder` in `actor.rs`

Integration tests:
- `integration::basic_session_prompt_response` — full round-trip prompt/response through forwarders

## Directory deleted -- session cleanup

```mermaid
sequenceDiagram
    participant T as Timer (60s)
    participant A as Actor

    T->>A: CwdHealthCheck
    Note over A: Iterate sessions, check cwd.exists()
    Note over A: Kill agent for deleted cwds
    Note over A: Remove from state file, persist
```

Source locations:
- **periodic timer spawn** — {anchor}`cwd-health-check-timer`
- **cleanup logic** — {anchor}`cwd-health-check`

Integration tests: *none yet*
