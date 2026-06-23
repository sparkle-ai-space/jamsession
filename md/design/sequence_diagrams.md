# Key sequence diagrams

Each page below walks through a major daemon flow with a mermaid diagram, step-by-step code walkthrough via `{anchor}` references, and a list of covering integration tests.

- [New session](./flow-new-session.md) — connect, initialize, create session, bridge
- [Reconnect (load/resume)](./flow-reconnect.md) — dead agent → respawn; alive agent → rewire
- [Message bridge](./flow-message-bridge.md) — steady-state bidirectional routing through forwarders
- [Idle spin-down](./flow-idle-spindown.md) — quiescence + idle timeout → agent kill
- [Agent crash](./flow-agent-crash.md) — detection and recovery
- [CWD health check](./flow-cwd-health.md) — periodic cleanup of deleted directories
