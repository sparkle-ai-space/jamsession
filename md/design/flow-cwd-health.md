# Directory deleted — session cleanup

The daemon periodically checks whether each session's working directory still exists. If a directory has been deleted (e.g., `rm -rf /tmp/project`), the session is removed from both memory and the persistent state file.

```mermaid
sequenceDiagram
    participant T as Timer (60s)
    participant D as Dispatcher

    T->>D: CwdHealthCheck
    Note over D: Iterate sessions, check cwd.exists()
    Note over D: Remove agent/client mappings for deleted cwds
    Note over D: Remove from state file, persist
```

## How it works

### Timer

A dedicated task sends `DispatcherMessage::CwdHealthCheck` every 60 seconds:

```{anchor}
cwd-health-check-timer
```

### Cleanup logic

The dispatcher iterates all sessions, identifies those whose `cwd` no longer exists, removes their agent and client mappings, removes them from the in-memory map and persistent state, and saves:

```{anchor}
cwd-health-check
```

## Integration tests

*None yet.*
