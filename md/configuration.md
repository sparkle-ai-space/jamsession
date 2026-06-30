# Configuration

Jamsession reads configuration from `~/.jamsession/config.toml` on startup. If the file doesn't exist, defaults are used.

## Config file

```toml
# Log verbosity: error, warn, info, debug, trace
log_level = "info"

# Agent configuration (pick one of the two forms below)
[agent]
# Use an agent registered in the acpr registry:
name = "claude-acp"

# Or use a custom binary:
# [agent.custom]
# path = "/usr/local/bin/my-agent"
# args = ["--verbose"]
# env = { MY_KEY = "value" }
```

### Agent

The `[agent]` section controls which agent process the daemon spawns for sessions.

| Field | Description |
|-------|-------------|
| `name` | Look up the agent by name in the [acpr](https://crates.io/crates/acpr) registry (default: `"claude-acp"`) |
| `custom.path` | Path to an agent binary (mutually exclusive with `name`) |
| `custom.args` | Arguments passed to the custom binary (optional) |
| `custom.env` | Environment variables set when launching the custom binary (optional) |

If neither `name` nor `custom` is specified, the daemon defaults to `name = "claude-acp"`.

### Log levels

| Level | What's logged |
|-------|--------------|
| `error` | Failures only |
| `warn` | Warnings + errors |
| `info` | Lifecycle events (agent spawn/kill, client connect/disconnect) |
| `debug` | Detailed lifecycle (timer starts, state transitions) |
| `trace` | Every ACP message flowing through the daemon |

## File locations

| Path | Purpose |
|------|---------|
| `~/.jamsession/daemon.sock` | Unix domain socket (created at startup, `0600` permissions) |
| `~/.jamsession/jamsession.db` | SQLite database for sessions and conversation history |
| `~/.jamsession/config.toml` | Daemon configuration |
| `~/.jamsession/daemon.log` | Main daemon log (daily rotation) |
| `~/.jamsession/sessions/<id>/session.log` | Per-session log |

## CLI options

```
jamsession [OPTIONS] [COMMAND]

Options:
    --config-dir <PATH>    Override the config/data directory (default: ~/.jamsession)
    -h, --help             Print help

Commands:
    daemon    Run the daemon (default)
    acp       Run as stdio ACP client (connects to daemon)

jamsession daemon [OPTIONS]

Options:
    --db-path <PATH>       Override the SQLite database location
    -h, --help             Print help
```

The `--config-dir` flag redirects all file paths (socket, database, config, logs) to the given directory. Useful for running isolated test instances.

## Environment variables

- `RUST_LOG` -- Overrides the `log_level` setting in config.toml (standard `tracing` filter syntax).

## Idle timeout

The agent idle timeout defaults to 15 minutes. After a client disconnects and 10 seconds of pipe silence pass (quiescence), the idle timer starts. When it expires, the agent process is killed.

The timeout is currently not user-configurable via config.toml (it can be overridden programmatically for integration tests).
