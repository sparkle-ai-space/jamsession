# Quick start guide

## Prerequisites

- Rust toolchain (stable)
- An ACP-compatible agent binary (e.g., `claude-acp` via the `acpr` registry)

## Building

```sh
cargo build --release
```

The binary is at `target/release/jamsession`.

## Running the daemon

Start the daemon directly:

```sh
jamsession daemon
```

Or let it auto-start -- when you run in ACP client mode and the daemon isn't running, it spawns automatically:

```sh
jamsession acp
```

The daemon listens on `~/.jamsession/daemon.sock` and logs to `~/.jamsession/daemon.log`.

## Creating a session

Connect to the daemon (via any ACP client) and send:

```json
{"jsonrpc": "2.0", "id": 1, "method": "session/new", "params": {"cwd": "/path/to/project", "additionalDirectories": [], "mcpServers": []}}
```

The daemon spawns an agent, initializes it, delivers interaction guidelines, and bridges your connection.

## Reconnecting to a session

If you disconnect and reconnect later:

```json
{"jsonrpc": "2.0", "id": 1, "method": "session/load", "params": {"sessionId": "...", "cwd": "/path/to/project", "mcpServers": []}}
```

The daemon respawns the agent (if needed), replays history, and bridges you back in.

To reconnect without receiving the history replay (e.g., your client already has it cached):

```json
{"jsonrpc": "2.0", "id": 1, "method": "session/resume", "params": {"sessionId": "...", "cwd": "/path/to/project", "mcpServers": []}}
```

## Listing sessions

```json
{"jsonrpc": "2.0", "id": 1, "method": "session/list", "params": {"cwd": null, "cursor": null}}
```

## ACP client mode

`jamsession acp` bridges stdin/stdout to the daemon socket, so you can use it as a drop-in ACP transport from any tool that expects stdio-based ACP:

```sh
jamsession acp
```

If the daemon isn't running, it will be started automatically.

## Debugging message flow

Enable trace recording in `~/.jamsession/config.toml`:

```toml
[daemon]
trace = true
```

Restart the daemon, exercise a session, then open the local trace viewer:

```sh
jamsession debug
```

The viewer listens on `http://127.0.0.1:3000` by default and live-polls trace rows from `~/.jamsession/jamsession.db`. Use `--session`, `--since`, `--today`, or `--ago 30m` to narrow the initial view.
