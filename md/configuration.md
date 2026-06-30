# Configuration

Jamsession reads configuration from `~/.jamsession/config.toml` on startup. If the file doesn't exist, defaults are used.

## Config file

```toml
[daemon]
# Log filter: supports tracing directives (e.g., "debug", "jamsession=debug,acpr=trace")
log_filter = "info"

# Idle timeout in seconds (default: 900 = 15 minutes)
idle_timeout_secs = 900

# Quiescence timeout in seconds (default: 10)
quiescence_timeout_secs = 10

# Default model to select when creating new sessions (optional).
# Uses session/set_config_option to set the model after session creation.
default-model = "default"

# Environment variables set on the daemon process at startup.
# These are inherited by all spawned agent processes.
[daemon.env]
CLAUDE_CODE_EXECUTABLE = "/path/to/claude"

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

### Daemon

The `[daemon]` section controls daemon-level settings that apply before any sessions are created.

| Field | Description |
|-------|-------------|
| `log_filter` | Tracing filter directive (default: `"info"`). Overridden by `RUST_LOG` env var if set. |
| `idle_timeout_secs` | Seconds of inactivity before killing the agent process (default: `900`) |
| `quiescence_timeout_secs` | Seconds of pipe silence after client disconnect before starting the idle timer (default: `10`) |
| `default-model` | Model to select via `session/set_config_option` after creating a session. Looks for the config option with `category: "model"`. Skipped if unset or if already the current value. |
| `env` | Key-value pairs set as environment variables on the daemon process at startup. Inherited by all spawned child processes (agents, npx, etc.). |

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

The `log_filter` field accepts any valid `tracing_subscriber::EnvFilter` directive:

| Value | What's logged |
|-------|--------------|
| `"error"` | Failures only |
| `"warn"` | Warnings + errors |
| `"info"` | Lifecycle events (agent spawn/kill, client connect/disconnect) |
| `"debug"` | Detailed lifecycle (timer starts, state transitions) |
| `"trace"` | Every ACP message flowing through the daemon |

You can also use per-crate filters like `"jamsession=debug,acpr=trace"`.

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

- `RUST_LOG` -- Overrides the `daemon.log_filter` setting in config.toml (standard `tracing` filter syntax).

## Idle timeout

The agent idle timeout defaults to 15 minutes (`idle_timeout_secs = 900`). After a client disconnects and the quiescence period passes (`quiescence_timeout_secs = 10`), the idle timer starts. When it expires, the agent process is killed.

Both values are configurable in the `[daemon]` section of config.toml.
