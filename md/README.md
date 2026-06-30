# What is Jamsession?

Jamsession is a daemon that manages the lifecycle of AI coding agents using the [Agent Client Protocol (ACP)](https://github.com/anthropics/agent-client-protocol). It sits between editor clients and agent processes, providing:

- **Session persistence** -- Agents can be killed and respawned without losing context. The daemon stores session metadata and conversation history in SQLite and replays history on reconnect.
- **Resource efficiency** -- Agent processes are ephemeral. They spin down after idle periods and spin back up on demand via `session/load`.
- **Single entry point** -- Clients connect to one Unix socket (`~/.jamsession/daemon.sock`). The daemon handles spawning, bridging, and lifecycle for all sessions.
- **One client per session** -- When a new client takes over a session, the previous one is disconnected automatically.

## How it works

```
 Editor/CLI          Daemon              Agent Process
 ──────────      ──────────────        ─────────────────
     │                 │                       │
     │── connect ─────>│                       │
     │<─ capabilities ─│                       │
     │── session/new ─>│── spawn ─────────────>│
     │                 │<─ initialize ─────────│
     │                 │── session/new ───────>│
     │<─ bridged ──────│<═══════ relay ═══════>│
     │                 │                       │
     │── disconnect ──>│                       │
     │                 │   (idle timeout)      │
     │                 │── kill ──────────────>│
     │                 │                       ✗
     │                 │                       
     │── reconnect ───>│                       │
     │<─ history ──────│                       │
     │                 │── spawn (new) ──────>│'
     │                 │── session/resume ────>│'
     │<─ bridged ──────│<═══════ relay ═══════>│'
```

The daemon is the sole ACP endpoint for clients -- they never communicate directly with agent processes.

## Project status

Academy is in active development. The core session lifecycle (create, load, resume, idle spin-down, respawn) is implemented. See the [quick start guide](./quickstart.md) to try it out.
