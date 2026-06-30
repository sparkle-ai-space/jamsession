# RFD 005: Message Trace & Debug Viewer

**Status**: Draft | **Date**: 2026-06-30

## Problem

When debugging daemon behavior (e.g., why a prompt returns empty text, why a session/update notification doesn't reach the client), we have no structured visibility into the messages flowing through the dispatcher. `RUST_LOG=debug` produces overwhelming, unstructured output that's hard to correlate across client/agent/session boundaries.

We need:
1. A machine-parsable trace of every dispatch flowing through the daemon's central actor.
2. A way to browse these traces after the fact (or live) in a human-friendly format.

## Proposal

### Message Trace (JSONL log)

The dispatcher records every dispatch it processes to a JSONL file:

```
~/.jamsession/traces/<session-id>.jsonl
```

Per-session files make it easy to view one session in isolation. A future addition could also log to a per-daemon-invocation file (`~/.jamsession/traces/daemon-<pid>.jsonl`) to observe cross-session multiplexing, but per-session is the starting point.

Each line is a JSON object:

```json
{
  "ts": "2026-06-30T10:16:57.086Z",
  "dir": "client->daemon",
  "client_id": 1,
  "session_id": "ec0c1eea-...",
  "kind": "request",
  "method": "session/prompt",
  "id": "29c9b583-...",
  "payload": { ... }
}
```

```json
{
  "ts": "2026-06-30T10:16:57.100Z",
  "dir": "agent->daemon",
  "agent_id": 1,
  "session_id": "ec0c1eea-...",
  "kind": "notification",
  "method": "session/update",
  "payload": { "sessionUpdate": "agent_message_chunk", "content": { "type": "text", "text": "Hello!" } }
}
```

```json
{
  "ts": "2026-06-30T10:16:57.100Z",
  "dir": "daemon->client",
  "client_id": 1,
  "session_id": "ec0c1eea-...",
  "kind": "notification",
  "method": "session/update",
  "payload": { "sessionUpdate": "agent_message_chunk", "content": { "type": "text", "text": "Hello!" } }
}
```

Fields:
- `ts` — ISO 8601 timestamp
- `dir` — one of `client->daemon`, `daemon->agent`, `agent->daemon`, `daemon->client`, `daemon-internal` (for internal events like model set)
- `client_id` / `agent_id` — numeric IDs assigned by the dispatcher
- `session_id` — the ACP session ID (if known at that point)
- `kind` — `request`, `response`, `notification`
- `method` — the JSON-RPC method name
- `id` — request/response correlation ID (if applicable)
- `payload` — the full JSON-RPC params/result/error (or a truncated summary for large payloads)

### Trace Points

A `Dispatch` in ACP is an enum covering requests, notifications, *and* responses — so each trace point captures all three message kinds.

The trace is recorded at these points in the dispatcher:

1. **FromClient** — when `handle_from_client` receives a dispatch (requests, notifications, or responses)
2. **route_to_agent** — when forwarding a dispatch to an agent
3. **FromAgent** — when `handle_from_agent` receives a dispatch from the agent forwarder (notifications, responses flowing back to the client)
4. **daemon->client** — when forwarding a dispatch to a client's `outgoing_tx`
5. **Internal events** — model set, session create/load, agent spawn/kill

### Configuration

```toml
[daemon]
# Enable message tracing (default: false in release, true in debug?)
trace = true

# Maximum payload size before truncation (bytes, default: 4096)
trace_max_payload = 4096
```

Or always-on with rotation/size limits.

### Debug Viewer: `jamsession debug`

A subcommand that serves a small web page (localhost only) rendering the trace files:

```
jamsession debug [--port 8787] [--session <id>]
```

The viewer shows:
- A timeline/sequence diagram of messages
- Filtering by session, method, direction
- Expandable payloads
- Color-coded by direction (client=blue, agent=green, internal=gray)
- Correlation: clicking a request highlights its response

Implementation:
- Static HTML + inline JS (no build step), served from an embedded `include_str!`
- Default port: 3000

## Open Questions

1. **Always-on vs opt-in?** Traces could always be written (with rotation) or only when `trace = true`. Always-on is better for debugging production issues after the fact.

2. **Per-session vs single file?** Per-session is easier to browse; single file is simpler to implement and allows seeing cross-session interleaving.

3. **Payload truncation** — large payloads (e.g., available_commands_update with 50 skills) should probably be truncated or summarized. How to handle this cleanly?

4. **Live tailing?** Should `jamsession debug` support live-tailing (watching for new messages as they arrive), or just static file viewing?

5. **Retention** — how long to keep traces? Per-session with same lifetime as the session log? Or a fixed rolling window?

## Non-Goals

- Modifying dispatches (this is read-only observability)
- Recording message *content* for replay (just enough to understand the flow)
- Performance profiling (this is about correctness debugging)

## Immediate Use Case

Understanding why `rhaicp`'s `s.prompt("Hi, who is this?")` returns empty text when talking to a real agent through the daemon. With traces, we'd immediately see whether:
- The prompt reaches the agent
- The agent sends back `agent_message_chunk` notifications
- Those notifications reach the dispatcher
- The dispatcher forwards them to the client
- The client receives them
