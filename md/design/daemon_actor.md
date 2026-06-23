# Daemon actor architecture

The daemon uses a **central actor** pattern: a single task owns all mutable session state and processes events sequentially from an mpsc channel. Client connections, agent connections, and timers all communicate with the actor by sending messages into this channel. This eliminates shared mutable state (`Arc<Mutex<...>>`) and ensures lifecycle transitions are race-free.

## Architecture overview

```mermaid
graph TD
Listener[Unix Socket Listener]

subgraph Clients
    CT1[Client Task<br>per-connection]
    CT2[Client Task<br>per-connection]
end

subgraph Agents
    Agent1[Agent Task<br>session A]
    Agent2[Agent Task<br>session B]
end

subgraph Timers
    Timer1[Timer A]
    Timer2[Timer B]
end

Actor[Daemon Actor<br>- sessions map<br>- state file<br>- lifecycle]    
Listener -->|accept| CT1
Listener -->|accept| CT2
CT1 -->|actor_tx| Actor
CT2 -->|actor_tx| Actor
Agent1 -->|actor_tx| Actor
Agent2 -->|actor_tx| Actor
Timer1 -->|actor_tx| Actor
Timer2 -->|actor_tx| Actor
Actor -.->|send_proxied_message| Agent1
Actor -.->|send_proxied_message| Agent2
Actor -.->|send_proxied_message| CT1
Actor -.->|send_proxied_message| CT2
```

The key invariant: **only the actor task reads or writes session state**. Everything else sends a `DaemonMessage` and (optionally) awaits a reply via a oneshot channel.

## Message types

The actor has two distinct enums: **`DaemonMessage`** (inputs that drive the actor) and **`LifecycleEvent`** (outcomes emitted for observers).

### `DaemonMessage` — inputs to the actor

```{anchor}
daemon-message
```

**Key design constraint**: `send_request(...).block_task().await` can only be called from within `cx.spawn()` or a `connect_with` callback — NOT from the actor task. Therefore agent spawn + initialize + session/new must run in the client handler task ({anchor}`handle-session-new`). The actor receives the results as fire-and-forget registration messages (`SessionCreated`, `SessionReconnected`).

### `LifecycleEvent` — outcomes for observers

```{anchor}
lifecycle-event
```

The actor emits `LifecycleEvent` values into a separate unbounded channel for subscribers (tests, tracing). These are purely observational — the actor never receives them back.

## Fresh connection — new session (internal)

```mermaid
sequenceDiagram
    participant CT as Client Task
    participant A as Actor
    participant Ag as Agent

    Note over CT: Client connects via Unix socket

    CT->>A: Initialize { req, reply }
    A-->>CT: capabilities (from cache or temp agent)

    CT->>A: ListSessions { reply }
    A-->>CT: sessions

    Note over CT: session/new received, runs in cx.spawn()
    CT->>Ag: spawn_agent_connection + initialize_agent
    CT->>Ag: new_session_on_agent
    Ag-->>CT: sessionId
    Note over CT: Install ClientForwarder + AgentForwarder
    CT->>A: SessionCreated { session_id, cwd, client_cx, agent_cx }
    Note over A: Persist state, store LiveSession
    CT-->>CT: respond to client

    Note over CT,Ag: Bridge active: client ↔ forwarder ↔ actor ↔ forwarder ↔ agent
```

Source: {anchor}`handle-session-new`

## Reconnect — load session, agent dead (internal)

```mermaid
sequenceDiagram
    participant CT as Client Task
    participant A as Actor
    participant Ag as Agent

    Note over CT: session/load received, runs in cx.spawn()
    CT->>A: QuerySessionState { session_id, reply }
    A-->>CT: { agent_dead: true, cwd }
    CT->>Ag: spawn + initialize
    CT->>Ag: load_session_on_agent
    Ag-->>CT: replay notifications
    Note over CT: Forward replay to client, install forwarders
    CT->>A: SessionReconnected { session_id, client_cx, agent_cx }
    CT-->>CT: respond to client
```

Source: {anchor}`handle-session-load`

## Reconnect — load session, agent alive (internal)

```mermaid
sequenceDiagram
    participant CT as Client Task (new)
    participant A as Actor
    participant Ag as Agent

    Note over CT: session/load received
    CT->>A: QuerySessionState { session_id, reply }
    A-->>CT: { agent_dead: false }
    Note over CT: Install ClientForwarder (no spawn)
    CT->>A: SessionReconnected { session_id, client_cx, replay_to_client: true }
    Note over A: Replay buffered notifications to new client_cx
    CT-->>CT: respond to client

    Note over CT,Ag: New client bridged to live agent
```

Source: {anchor}`handle-session-load`

## Client disconnect and idle spin-down (internal)

```mermaid
sequenceDiagram
    participant CT as Client Task
    participant A as Actor
    participant T as Timer

    Note over CT: TCP/socket closes (connect_to returns)
    CT->>A: ClientDisconnected { session_id }
    Note over A: Drop client_cx, bump generation
    Note over A: Spawn quiescence timer with current generation

    T->>A: AgentQuiescent { session_id, generation }
    Note over A: generation matches → Quiescent
    Note over A: Spawn idle timer with same generation

    T->>A: IdleTimeoutElapsed { session_id, generation }
    Note over A: generation matches → kill agent, clear buffer
```

Source: {anchor}`disconnect-and-idle`

## Agent crash detection (internal)

```mermaid
sequenceDiagram
    participant Ag as Agent
    participant A as Actor

    Note over Ag: Process exits unexpectedly
    Ag--xA: AgentExited { session_id }
    Note over A: Mark AgentDead, clear buffer
```

Source: {anchor}`handle-agent-exited`

Note: respawn is not currently implemented in the actor (requires independent agent connections).

## Message flow through the bridge

During normal operation, forwarders route messages through the actor:

```mermaid
sequenceDiagram
    participant C as Client
    participant CF as ClientForwarder
    participant A as Actor
    participant AF as AgentForwarder
    participant Ag as Agent

    C->>CF: prompt/start [Dispatch]
    CF->>A: ClientMessage { session_id, dispatch }
    A->>Ag: agent_cx.send_proxied_message(dispatch)

    Ag->>AF: session/update [Dispatch]
    AF->>A: AgentMessage { session_id, dispatch }
    Note over A: Buffer notification, bump generation
    A->>C: client_cx.send_proxied_message(dispatch)

    Ag->>AF: prompt/end [Dispatch]
    AF->>A: AgentMessage { session_id, dispatch }
    A->>C: client_cx.send_proxied_message(dispatch)
```

Source: {anchor}`route-messages`

## Design principles

**Single writer**: The actor is the sole owner of `sessions: HashMap<String, LiveSession>`. No mutexes needed for session state.

**Inputs vs events**: `DaemonMessage` is what the actor processes; `DaemonEvent` is what it emits. Tests subscribe to the event channel and assert on outcomes. The two enums are cleanly separated — no variant appears in both.

**Request-reply via oneshot**: Client tasks that need a response (e.g., `SessionNew`) include a `tokio::sync::oneshot::Sender` in the message. The actor computes the answer and sends it back. The client task awaits the oneshot.

**Fire-and-forget for events**: Messages like `ClientDisconnected`, `AgentExited`, and timer expirations don't need replies — the actor handles them unilaterally.

**Timers as messages**: Instead of spawning timer tasks that grab mutexes, the actor spawns lightweight tasks that simply sleep and then send `AgentQuiescent` / `IdleTimeoutElapsed` back to the actor channel. The actor checks whether the timer is still relevant (client may have reconnected) before acting.

**Agent task isolation**: Each live agent has a dedicated task that reads from the agent's stdio and sends `AgentMessage { session_id, message }` into the actor channel. The actor decides what to do with each message (buffer it, forward to client, update lifecycle state). If the agent process exits, the task sends `AgentExited`.
