# Message Trace & Debug Viewer

## TL;DR

- Record every ACP dispatch flowing through the daemon's central actor to a `traces` table in SQLite.
- Capture responses via wrapped responders routed back through the actor loop — giving canonical ordering.
- Serve an interactive sequence-diagram viewer via `jamsession debug` on localhost:3000.
- Opt-in via `trace = true` in config. Traces retained as long as session data.

## Motivation

When debugging daemon behavior (e.g., why a prompt returns empty text, why a `session/update` notification doesn't reach the client), we have no structured visibility into the messages flowing through the dispatcher. `RUST_LOG=debug` produces overwhelming, unstructured output that's hard to correlate across client/agent/session boundaries.

We need:
1. A machine-parsable trace of every dispatch flowing through the daemon's central actor.
2. A way to browse these traces after the fact (or live) in a human-friendly format.

## Change in a nutshell

Add a `traces` table to the existing `jamsession.db`. When `trace = true`, the dispatcher records every dispatch and lifecycle event as a row. A new `jamsession debug` subcommand serves a web-based sequence diagram viewer that queries this table.

The key design choice: responses to locally-handled requests (like `session/new`) are captured by wrapping the `Responder` — the wrapper routes the response back through the actor loop before delivering it on the wire. This gives a single canonical ordering for all trace events.

## Detailed plans

### Storage: SQLite

Traces are stored in the existing `jamsession.db` database alongside session data:
- Queryable (filter by session, method, direction, time range)
- Live tailing via poll (`SELECT ... WHERE rowid > ? LIMIT ...` on 200ms interval)
- Natural retention — traces live as long as session data. When a session is archived or deleted (via `session/delete`), its trace rows are removed too.
- No additional dependencies or file management

#### Schema

```sql
CREATE TABLE traces (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,                    -- ISO 8601 timestamp
    session_id TEXT,                     -- ACP session ID (NULL for unrouted)
    dir TEXT NOT NULL,                   -- client_to_daemon, daemon_to_agent, agent_to_daemon, daemon_to_client, internal
    role TEXT,                           -- source/target role: "acp-client", "github", "agent", etc.
    kind TEXT NOT NULL,                  -- request, response, notification, event
    method TEXT,                         -- JSON-RPC method name
    request_id TEXT,                     -- JSON-RPC request ID (from responder.id() / router.id())
    payload TEXT NOT NULL                -- full JSON-RPC params/result/error as JSON
);

CREATE INDEX idx_traces_session ON traces(session_id);
CREATE INDEX idx_traces_ts ON traces(ts);
```

#### Participants and roles

A session may have multiple concurrent clients (e.g., an ACP client in a terminal and a GitHub integration posting comments). Instead of opaque numeric IDs, each participant is identified by a **role** string:

- `"acp-client"` — a standard ACP client connected via the Unix socket
- `"github"` — the GitHub integration (future)
- `"agent"` — the agent process

Lifecycle events (connect, disconnect) are recorded as `kind: "event"` rows, so the trace shows when participants come and go without needing to track IDs across rows.

#### Record format

Each row corresponds to one dispatch or lifecycle event passing through the dispatcher:

| Field | Description |
|-------|-------------|
| `ts` | ISO 8601 timestamp with millisecond precision |
| `session_id` | ACP session ID (NULL if not yet associated) |
| `dir` | Direction: `client_to_daemon`, `daemon_to_agent`, `agent_to_daemon`, `daemon_to_client`, `internal` |
| `role` | The participant role (e.g., `"acp-client"`, `"github"`, `"agent"`) |
| `kind` | `request`, `response`, `notification`, or `event` (for lifecycle: connect, disconnect, model set, etc.) |
| `method` | JSON-RPC method name (e.g., `session/prompt`, `session/update`) or event name (e.g., `client_connected`, `client_disconnected`) |
| `request_id` | JSON-RPC request ID for correlating requests with responses (from `responder.id()` for requests, `router.id()` for responses) |
| `payload` | Full JSON-RPC params/result/error — no truncation |

### Trace entry kinds

There are two root categories of trace entries:

1. **Messages** — directed communication from one participant to another (`a → b`). These have a `dir` indicating source and destination.
2. **Events** — things that happen locally within a participant (no direction). These represent state changes, lifecycle transitions, or internal decisions.

Both use the same table row; the `kind` field discriminates:

#### Messages (`dir` is set)

| `kind` | Meaning | Example |
|--------|---------|---------|
| `request` | JSON-RPC request from source to destination | Client sends `session/prompt` to daemon |
| `response` | JSON-RPC response flowing back | Daemon returns prompt result to client |
| `notification` | JSON-RPC notification (no response expected) | Agent sends `session/update` to daemon |

A `Dispatch` in ACP is an enum covering all three — so wherever we intercept a dispatch, we record one row.

#### Events (`dir` is `internal`, no destination)

| `kind` | Meaning | Example |
|--------|---------|---------|
| `event` | Lifecycle or internal state change | `client_connected`, `client_disconnected`, `agent_spawned`, `agent_killed_idle`, `model_set`, `session_created` |

Events use `method` as the event name and `payload` for any relevant context (e.g., `{"model": "default"}` for `model_set`).

This is extensible — new event names can be added without schema changes. New message kinds (if ACP ever adds them) would just be new `kind` values.

### Response capture via wrapped responders

When the dispatcher handles a request itself (e.g., `session/new`, `initialize`, `session/list`), it calls `responder.respond(...)` directly. That response goes straight to the client via an internal oneshot channel — it never passes through the central actor loop, so it would be invisible to tracing.

The solution: **wrap the responder using `Responder::wrap_params`**. In the dispatcher, all dispatches arrive as `Dispatch<UntypedMessage, UntypedMessage>` with `Responder<serde_json::Value>` — the type erasure has already happened at the transport layer. Before passing the dispatch into `MatchDispatch`, if tracing is enabled and the dispatch is a `Request`, we wrap its responder:

```rust
if let Dispatch::Request(ref msg, responder) = dispatch {
    let tx = self.dispatcher_tx.clone();
    let method = msg.method.clone();
    let request_id = responder.id();
    let wrapped = responder.wrap_params(move |_method, result| {
        let payload = serialize_response(&result);
        let _ = tx.send(DispatcherMessage::ResponseSent { method, request_id, payload });
        result // pass through unchanged — original delivery still happens
    });
    dispatch = Dispatch::Request(msg, wrapped);
}
```

The `wrap_params` closure runs synchronously inside `respond_with_result`. Since `dispatcher_tx` is an unbounded channel, the send is non-blocking — no async needed. The response still delivers to the wire immediately; the closure just piggybacks a notification to the actor loop.

When `ResponseSent` arrives in the actor loop, it records the trace row. The row appears *after* the response is on the wire, but it's still in canonical order from the actor's perspective — the response trace appears after all the internal events that produced it.

Events from spawned tasks (like `model_set` inside `agent_pipe`) use the same pattern: they send a `DispatcherMessage` variant back to the actor loop, which records the trace.

### Trace points in the dispatcher

The trace is recorded at these points — all within the central actor loop:

| Point | Category | `dir` | `role` | What's captured |
|-------|----------|-------|--------|-----------------|
| `handle_from_client` receives dispatch | message | `client_to_daemon` | source role (e.g., `acp-client`) | All incoming client dispatches (requests, notifications, responses) |
| `route_to_agent` forwards dispatch | message | `daemon_to_agent` | `agent` | Dispatches forwarded to the agent |
| `handle_from_agent` receives dispatch | message | `agent_to_daemon` | `agent` | Agent notifications and responses flowing back |
| Forward to client's `outgoing_tx` | message | `daemon_to_client` | target role | Dispatches delivered to clients |
| `ResponseReady` arrives in actor loop | message | `daemon_to_client` | target role | Responses to locally-handled requests (session/new, initialize, etc.) |
| Client connects | event | `internal` | connecting role | `client_connected` |
| Client disconnects | event | `internal` | disconnecting role | `client_disconnected` |
| Agent spawned | event | `internal` | `agent` | `agent_spawned` |
| Agent quiescent | event | `internal` | `agent` | `agent_quiescent` |
| Agent killed (idle/crash) | event | `internal` | `agent` | `agent_killed_idle` / `agent_crashed` |
| Model set | event | `internal` | `daemon` | `model_set` |
| Session created/loaded/resumed | event | `internal` | `daemon` | `session_created` / `session_loaded` / `session_resumed` |

### Configuration

```toml
[daemon]
# Enable message tracing (default: false)
trace = true
```

Tracing is opt-in. When disabled, no trace rows are written to the database.

### Debug Viewer: `jamsession debug`

A subcommand that serves a web page (localhost only) rendering trace data from the database:

```
jamsession debug [--port 3000] [--session <id>] [--since <time>] [--today] [--ago <duration>]
```

Time filters:
- `--since 2026-06-30T10:00:00` — absolute timestamp (parsed via `jiff`)
- `--today` — shorthand for midnight today
- `--ago 1h` — relative duration (e.g., `30m`, `2h`, `1d`)

The viewer shows:
- A timeline/sequence diagram of messages
- Live tailing (polls DB every 200ms for new rows)
- Filtering by session, method, direction
- Expandable payloads
- Color-coded by direction (client=blue, agent=green, internal=gray)
- Correlation: clicking a request highlights its response

Implementation:
- Static HTML + inline JS (no build step), served from an embedded `include_str!`
- JSON API endpoints for the viewer to query traces from the DB
- Default port: 3000
- Visual inspiration: `agent-client-protocol-trace-viewer` in the acp-rust-sdk repo — renders SVG sequence diagrams with rainbow-colored request/response pairs, timeline spans showing processing duration, delta times between events, and inline content previews for `session/update` notifications. Same dark-theme aesthetic (VS Code-like). We can reuse the same rendering approach (vanilla JS, SVG swimlanes) adapted for our schema.

### Example: Full session lifecycle

This walkthrough shows the trace rows generated for a typical session:
1. Client connects, creates a session, sends a prompt, gets a response
2. Client disconnects while agent is still alive
3. Agent finishes its turn and goes quiescent
4. Client reconnects and sends another prompt

Session ID: `sess-abc123`

#### Phase 1: Client connects and prompts

| id | ts | session_id | dir | role | kind | method | request_id | payload (abbreviated) |
|----|-----|-----------|-----|------|------|--------|----------------|----------------------|
| 1 | ...000 | NULL | internal | acp-client | event | client_connected | NULL | `{}` |
| 2 | ...001 | NULL | client_to_daemon | acp-client | request | session/new | req-1 | `{"cwd": "/home/user/project"}` |
| 3 | ...002 | sess-abc123 | internal | daemon | event | session_created | NULL | `{"session_id": "sess-abc123"}` |
| 4 | ...003 | sess-abc123 | internal | agent | event | agent_spawned | NULL | `{}` |
| 5 | ...004 | sess-abc123 | internal | daemon | event | model_set | NULL | `{"from": "claude-opus-4-8", "to": "default"}` |
| 6 | ...050 | sess-abc123 | daemon_to_client | acp-client | response | session/new | req-1 | `{"sessionId": "sess-abc123"}` |
| 7 | ...051 | sess-abc123 | client_to_daemon | acp-client | request | session/prompt | req-2 | `{"prompt": [{"type": "text", "text": "Hello!"}]}` |
| 8 | ...052 | sess-abc123 | daemon_to_agent | agent | request | session/prompt | req-2 | `{"prompt": [{"type": "text", "text": "Hello!"}]}` |
| 9 | ...100 | sess-abc123 | agent_to_daemon | agent | notification | session/update | NULL | `{"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "Hi there!"}}` |
| 10 | ...100 | sess-abc123 | daemon_to_client | acp-client | notification | session/update | NULL | `{"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "Hi there!"}}` |
| 11 | ...200 | sess-abc123 | agent_to_daemon | agent | response | session/prompt | req-2 | `{"result": null}` |
| 12 | ...200 | sess-abc123 | daemon_to_client | acp-client | response | session/prompt | req-2 | `{"result": null}` |

#### Phase 2: Client disconnects, agent continues working

The client closes its socket. The agent is still alive (maybe doing background work or awaiting the next prompt).

| id | ts | session_id | dir | role | kind | method | request_id | payload (abbreviated) |
|----|-----|-----------|-----|------|------|--------|----------------|----------------------|
| 13 | ...300 | sess-abc123 | internal | acp-client | event | client_disconnected | NULL | `{}` |

Note: if the agent sends notifications after the client disconnects, they're still recorded (they go into the buffer) but there's no `daemon_to_client` row since no client is connected:

| id | ts | session_id | dir | role | kind | method | request_id | payload (abbreviated) |
|----|-----|-----------|-----|------|------|--------|----------------|----------------------|
| 14 | ...350 | sess-abc123 | agent_to_daemon | agent | notification | session/update | NULL | `{"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "(still thinking...)"}}` |

#### Phase 3: Agent goes quiescent

After the quiescence timeout (10s of silence), the daemon marks the agent quiescent. The idle timer starts.

| id | ts | session_id | dir | role | kind | method | request_id | payload (abbreviated) |
|----|-----|-----------|-----|------|------|--------|----------------|----------------------|
| 15 | ...10300 | sess-abc123 | internal | agent | event | agent_quiescent | NULL | `{}` |

#### Phase 4: Client reconnects and sends another prompt

A new client connects and resumes the session.

| id | ts | session_id | dir | role | kind | method | request_id | payload (abbreviated) |
|----|-----|-----------|-----|------|------|--------|----------------|----------------------|
| 16 | ...15000 | NULL | internal | acp-client | event | client_connected | NULL | `{}` |
| 17 | ...15001 | sess-abc123 | client_to_daemon | acp-client | request | session/resume | req-3 | `{"sessionId": "sess-abc123"}` |
| 18 | ...15002 | sess-abc123 | daemon_to_client | acp-client | response | session/resume | req-3 | `{}` |
| 19 | ...15010 | sess-abc123 | client_to_daemon | acp-client | request | session/prompt | req-4 | `{"prompt": [{"type": "text", "text": "What were you thinking about?"}]}` |
| 20 | ...15011 | sess-abc123 | daemon_to_agent | agent | request | session/prompt | req-4 | `{"prompt": [{"type": "text", "text": "What were you thinking about?"}]}` |
| 21 | ...15100 | sess-abc123 | agent_to_daemon | agent | notification | session/update | NULL | `{"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "I was considering..."}}` |
| 22 | ...15100 | sess-abc123 | daemon_to_client | acp-client | notification | session/update | NULL | `{"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "I was considering..."}}` |
| 23 | ...15200 | sess-abc123 | agent_to_daemon | agent | response | session/prompt | req-4 | `{"result": null}` |
| 24 | ...15200 | sess-abc123 | daemon_to_client | acp-client | response | session/prompt | req-4 | `{"result": null}` |

#### Key observations

- **Request/response correlation**: `request_id` lets you pair row 7 (client sends prompt) → row 8 (daemon forwards to agent) → row 11 (agent responds) → row 12 (daemon forwards to client).
- **Disconnected client**: Row 14 shows the agent sending a notification with no corresponding `daemon_to_client` row — the viewer can highlight this as "delivered to buffer only."
- **Session association**: Rows 1–2 have `session_id = NULL` because the session doesn't exist yet. Everything after row 3 is tagged with the session.
- **Extensibility**: Adding a new event (e.g., `idle_timer_started`) is just a new row with `kind = "event"` and a new method name. No schema migration needed.

## Frequently asked questions

### Why SQLite instead of JSONL files?

Queryability (filter by session/method/time without parsing), live tailing via poll, atomic writes, and natural retention tied to session lifetime. We already have SQLite for session persistence — one fewer dependency.

### Why route responses through the actor loop?

Responses to locally-handled requests (`session/new`, `initialize`, etc.) go directly from the dispatcher to the wire via `Responder::respond()`. Without wrapping, they'd be invisible to tracing. Routing through the actor loop gives us a single canonical ordering for all trace events — the row `id` order reflects causality. The latency cost (one mpsc hop) is negligible.

### Why not always-on?

Full payload recording for every dispatch can generate significant data. Opt-in keeps the default lean. In the future we could add a "lite" always-on mode that records only method/direction/timestamp without payloads.

### Why roles instead of numeric IDs?

Numeric IDs are opaque — they don't tell you what kind of participant sent a message. Roles give semantic meaning (`"acp-client"` vs `"github"` vs `"agent"`) and are sufficient when combined with lifecycle events (connect/disconnect) to reconstruct the timeline. If we later need to distinguish multiple concurrent clients of the same role, we can add an optional instance qualifier (e.g., `"acp-client:2"`).

## Implementation plan and status

### Step 1: Trace storage layer (insert + query)

**Type**: New functionality — needs tests.

Add the `traces` table to the SQLite schema and expose `Store::insert_trace` and `Store::query_traces` as a single testable unit. The query API supports filtering by session, time range, and cursor-based pagination (for live tailing).

**Test (red first)**: Insert several traces via `store.insert_trace(...)` with different sessions and timestamps, then call `store.query_traces(...)` with various filters and assert correct results. This tests the public `Store` interface — no internal DB details leak.

- [x] Add `CREATE TABLE traces` + indexes to `configure_sqlite_file`
- [x] Schema migration from v1 → v2
- [x] `TraceRecord` struct (the public query result type)
- [x] `Store::record_trace(...)` 
- [x] `Store::traces(TraceQuery) -> Vec<TraceRecord>`
- [x] `Store::delete_traces_for_session(session_id)` — called from existing `remove_session`
- [x] Tests: insert then query with each filter dimension; assert no-op when session deleted

### Step 2: `TraceRecorder` with config gating

**Type**: New functionality — needs tests.

A struct that wraps a `Store` and a `trace_enabled` flag. Provides semantic methods (`record_message`, `record_event`) that call `store.insert_trace`. No-ops when disabled. Also add the `trace` field to `DaemonConfig`.

**Test (red first)**: Create a recorder with an in-memory store, call `record_message(...)` and `record_event(...)`, then use `store.query_traces(...)` to verify the rows. Also test that a disabled recorder produces no rows. All tests go through the public `Store::query_traces` interface — no private state inspection.

- [x] Add `trace` field to `DaemonConfig` + `Config::trace_enabled()` accessor
- [x] Dispatcher trace helpers supersede `TraceRecorder::new(store, enabled)`
- [x] Dispatcher trace helpers record messages with `dir`, `role`, `session_id`, `kind`, `method`, `request_id`, and `payload`
- [x] Dispatcher trace helpers record events with `role`, `session_id`, event name, and payload
- [x] Tests: record → query → assert; disabled → query → assert empty

### Step 3: Extract trace metadata from `Dispatch`

**Type**: New functionality — needs test.

A helper `fn trace_metadata(dispatch: &Dispatch) -> (kind, method, request_id, payload)` that borrows a dispatch and extracts the fields needed for recording.

The `UntypedMessage` inside Request/Notification has public `method` and `params` fields. For Response, serialize the Result. The Responder isn't touched (only borrowed for its id).

**Test (red first)**: Construct each `Dispatch` variant, call `trace_metadata`, assert correct extraction. This is a pure function test — no DB, no dispatcher.

- [x] `trace_metadata(&Dispatch) -> TraceFields` 
- [x] Integration tests cover request, notification, and response trace extraction

### Step 4: Wire tracing into dispatcher — messages

**Type**: New functionality (additive, no behavior change to existing flows).

Add a `TraceRecorder` to the `Dispatcher`. At each message routing point, call `recorder.record_message(...)`. Thread `trace_enabled` from config through `Daemon` → `Dispatcher` → `TraceRecorder`.

**Prerequisite**: Expose a `Store` clone from `TestDaemon` so integration tests can query traces. Add `pub fn store(&self) -> Store` to `TestDaemon` (stash a clone during construction before passing it to the daemon).

**Test (red first)**: Integration test using `TestDaemon` — start daemon with tracing enabled, send a prompt via rhaicp, then call `daemon.store().query_traces(session_id)`. Assert we see the prompt flow: `client_to_daemon` request (`session/prompt`), `daemon_to_agent` request, `agent_to_daemon` notification(s), `daemon_to_client` notification(s), `agent_to_daemon` response, `daemon_to_client` response. (Note: `session/new` response is *not* asserted here — that requires the wrapped responder from Step 6.)

- [x] Tests query traces by reopening `Store` at the daemon database path
- [x] Dispatcher owns trace helper methods instead of a separate `TraceRecorder`
- [x] Thread config through `Daemon::with_trace_enabled` → `Dispatcher::new`
- [x] Trace in `handle_from_client` (client_to_daemon)
- [x] Trace in `route_to_agent` (daemon_to_agent)
- [x] Trace in `handle_from_agent` (agent_to_daemon)
- [x] Trace when forwarding to client's `outgoing_tx` (daemon_to_client)
- [x] Integration test asserting prompt trace row sequence

### Step 5: Wire tracing into dispatcher — lifecycle events

**Type**: New functionality — needs test.

Record events for connect/disconnect/spawn/quiescent/kill/session lifecycle.

**Test (red first)**: Integration test — start `TestDaemon` with tracing, create a session, disconnect client, wait for quiescent. Query traces and assert event rows appear in order: `client_connected`, `session_created`, `agent_spawned`, `client_disconnected`, `agent_quiescent`.

- [x] Trace `client_connected` in `ClientRegistered` handler
- [x] Trace `client_disconnected` in `handle_client_disconnected`
- [x] Trace `agent_spawned` in `handle_agent_ready`
- [x] Trace `agent_quiescent` in `handle_agent_quiescent`
- [x] Trace `agent_killed_idle` in `handle_idle_timeout`
- [x] Trace `session_created` / `session_loaded` / `session_resumed`
- [x] Add `DispatcherMessage::ModelSet` variant, trace `model_set` in actor loop

### Step 6: Wrapped responder for response capture

**Type**: New functionality — needs test.

When tracing is enabled, wrap incoming `Dispatch::Request` responders using `Responder::wrap_params` *before* passing into `MatchDispatch`. The closure synchronously sends `DispatcherMessage::ResponseSent` to the actor loop (unbounded channel send is non-blocking), then passes the response value through unchanged so original wire delivery still happens immediately.

Since dispatches in the dispatcher are `Dispatch<UntypedMessage, UntypedMessage>` with `Responder<serde_json::Value>`, there are no generics issues — the wrapping happens at the type-erased level before `MatchDispatch` downcasts to typed handlers.

**Test (red first)**: Integration test — send `session/new` to a traced daemon, query traces via `daemon.store().query_traces(session_id)`, assert there's a `daemon_to_client` response row for `session/new` with a `request_id` matching the corresponding request row.

- [x] Add `DispatcherMessage::ResponseSent { method, request_id, session_id, payload }`
- [x] In `handle_from_client`, wrap typed local responders when tracing is enabled
- [x] Handle `ResponseSent` in actor loop: record `daemon_to_client` response trace row
- [x] Integration test: `session/new` request row + response row both present with matching `request_id`

### Step 7: `jamsession debug` subcommand — server skeleton

**Type**: New functionality — needs test.

Add the `Debug` variant to the CLI. Localhost server with `GET /` (embedded HTML) and `GET /api/traces?session=...&after_id=...&since=...`.

**Test (red first)**: Start the server programmatically in a test, insert some traces into the store, fetch `/api/traces?session=X`, assert the JSON response matches what `store.query_traces` would return. This tests the HTTP layer as a public interface without mocking.

- [x] Add `Debug` command with `--port`, `--session`, `--since`, `--today`, `--ago` args
- [x] Time parsing via existing `chrono` dependency
- [x] Localhost server: `GET /` serves embedded HTML, `GET /api/traces` queries store
- [x] Integration test: insert traces → HTTP GET → assert JSON

### Step 8: Debug viewer HTML/JS

**Type**: New functionality — manual verification (visual).

Embedded single-page app. Polls `/api/traces?after_id=N` every 200ms for live tailing.

No automated tests — verify by running `jamsession debug` against a real or test session.

- [x] `viewer.html` with inline CSS/JS
- [x] SVG swimlane rendering (client / daemon / agent columns)
- [x] Rainbow request/response correlation
- [x] Expandable payload panel on click
- [x] Filter controls (session picker, method filter)
- [x] Live tail toggle

### Step 9: Use it to debug the empty-response issue

**Type**: Investigation — no code changes expected.

Run the live agent test with `trace = true`, open the viewer, identify where the response text goes missing.

- [x] Run test, capture trace
- [x] Document findings

## Implementation progress log

### 2026-07-01

Current production slice:

- Added the `traces` table and indexes in the existing SQLite database setup.
- Added `Store::record_trace`, `Store::traces`, `TraceQuery`, and `TraceRecord`.
- Added `trace = true` to `[daemon]` config and threaded it through `Daemon` into the dispatcher.
- Recorded opt-in trace rows for client/agent dispatch flow, local daemon responses, and lifecycle events.
- Added `jamsession debug` with `--port`, `--session`, `--since`, `--today`, and `--ago`.
- Added a localhost-only static HTML/JS viewer that polls `/api/traces` every 200ms and supports session/method/direction filters.
- Documented `trace` and `jamsession debug` in the user and design guides.

Deviations and surprises:

- Trace storage is schema version `2`; opening a v1 database creates the trace table and updates `schema_version`.
- I did not introduce a separate `TraceRecorder` type. The dispatcher now has small trace helper methods over `Store`, which kept the change narrower.
- `Responder::wrap_params` is available, but by the time the dispatcher uses `MatchDispatch`, responders are typed. The implementation wraps the typed local responders in each local request arm rather than doing one type-erased pre-match transform.
- Successful response payloads are serialized via `JsonRpcResponse::into_json`. Error response traces preserve a structured `{ "error": "..." }` string because the SDK does not expose a stable public JSON representation for errors at this interception point.
- The debug server is dependency-free over `tokio::net::TcpListener`, not Axum. That avoided a new direct web dependency for two simple routes.
- Time parsing uses existing `chrono` instead of adding `jiff`.
- The viewer now includes SVG swimlanes, colored request/response correlation, clickable row highlighting, expandable payloads, and a live-tail toggle.
- `model_set` is traced through the actor loop with `DispatcherMessage::ModelSet`.

Review follow-ups:

- Added trace coverage for alive `session/load` and `session/resume`, not just respawned load/resume.
- Removed the cwd-cleanup `session_deleted` trace row so deleted sessions do not retain trace rows.
- Replaced viewer `innerHTML` rendering with DOM construction and `textContent` for trace payloads.

Verification so far:

- `cargo fmt --check`
- `cargo clippy --all --workspace`
- `cargo test --all --workspace`
- `mdbook build`

Follow-up slice:

- Added `DispatcherMessage::ModelSet` so successful model configuration changes are recorded as actor-loop trace events.
- Expanded the debug viewer from a table into SVG swimlanes with colored request/response correlation, clickable row highlighting, expandable payloads, and a live-tail toggle.
- Added an HTTP-level test for `/api/traces?session=...`.

Live-agent investigation:

- Ran `cargo test -p jamsession-test live_agent_responds_to_prompt -- --ignored --nocapture` with trace enabled.
- The first run failed during startup guideline delivery because the live Claude ACP agent returned `Authentication required`; no user prompt reached the daemon.
- Reran with guideline injection disabled for this ignored diagnostic test. The trace captured `session/new`, `model_set`, `session_created`, an agent `session/update`, the user `session/prompt`, daemon forwarding to the agent, an agent error response, daemon forwarding the error to the client, a usage update, and client disconnect.
- The empty-response issue could not be reproduced because the live agent rejected the prompt with `Authentication required`.
- The trace did reveal a correlation caveat: ACP proxying sends daemon-to-agent requests through `send_request_to`, which assigns a fresh SDK request ID for the agent leg. As a result, the client-side `session/prompt` request ID and the agent-side response request ID differ in the trace. The response still routes correctly, but strict end-to-end request ID equality is not available at the dispatcher interception points.
