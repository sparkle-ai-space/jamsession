# Idle spin-down

When all clients disconnect from a session, the daemon doesn't kill the agent immediately. It waits through two phases — quiescence (is the pipe truly silent?) and idle timeout (has enough wall-clock time passed?) — before terminating the agent process.

```mermaid
sequenceDiagram
    participant CP as Client Pipe
    participant D as Dispatcher
    participant T as Timer

    Note over CP: Socket closes (connect_with returns)
    CP->>D: ClientDisconnected { client_id }
    Note over D: Remove client_id from session.client_ids
    Note over D: All clients gone → bump generation, spawn quiescence timer

    T->>D: AgentQuiescent { session_id, generation }
    Note over D: Check generation matches (stale → discard)
    Note over D: Transition to Quiescent, spawn idle timer

    T->>D: IdleTimeoutElapsed { session_id, generation }
    Note over D: Check generation matches
    Note over D: Kill agent (drop AgentHandle), clear buffer
```

## How it works

### Client disconnect detection

When the ACP `connect_with(transport).await` returns (socket closed), the `EofSignalingTransport` triggers the outgoing stream to end. The client pipe then sends `ClientDisconnected { client_id }` to the dispatcher before exiting.

```{anchor}
client-disconnect
```

### Generation-counter timer pattern

The dispatcher doesn't track or abort timer tasks. Instead, each session has a monotonic `generation` counter that increments on every state change (client connects/disconnects, message activity, reconnect). Timer tasks carry the generation they were spawned at. When the timer fires and sends its message back to the dispatcher, the dispatcher compares generations — if they differ, the timer is stale and discarded.

This eliminates the need for `JoinHandle` tracking or `.abort()` calls. Sleeping tasks are harmless — they just produce ignored messages.

```{anchor}
disconnect-and-idle
```

### Lifecycle state transitions

```text
Active → (all clients disconnect) → spawn quiescence timer
       → AgentQuiescent (gen matches) → Quiescent → spawn idle timer
       → IdleTimeoutElapsed (gen matches) → kill agent → AgentDead
```

If a client reconnects at any point, the generation bumps and both timers become stale.

## Integration tests

- `integration::agent_killed_after_idle_timeout` *(ignored — requires independent agent connections)*

## Known limitation

The idle spin-down cannot be tested with in-process `RhaiAgent` because `spawn_connection` ties the agent's lifetime to the parent client connection. When the client disconnects, the parent ACP connection doesn't close (it waits for the child agent connection), so `ClientDisconnected` is never emitted. This requires independent agent connections to resolve.
