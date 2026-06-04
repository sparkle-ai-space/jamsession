# Contract: TestDaemon API

The test harness API that integration tests use to start isolated daemon instances and run scripted clients.

## TestDaemon

```rust
/// An isolated daemon instance for integration testing.
/// Implements Drop to ensure cleanup even on panic.
pub struct TestDaemon {
    // private fields
}

impl TestDaemon {
    /// Start a test daemon with the given configuration.
    /// Panics if the daemon doesn't start within 2 seconds.
    pub async fn start(config: TestDaemonConfig) -> Self;

    /// Execute a Rhai client script against this daemon.
    /// Returns the script's last expression as a string.
    pub async fn execute_client(&self, script: &str) -> String;

    /// Execute a Rhai client script with a specific cwd.
    pub async fn execute_client_with_cwd(&self, script: &str, cwd: &Path) -> String;

    /// Get the socket path (for custom connection logic).
    pub fn socket_path(&self) -> &Path;

    /// Get the state directory path.
    pub fn state_dir(&self) -> &Path;
}

impl Drop for TestDaemon {
    /// Kills daemon task and cleans up temp directory.
    fn drop(&mut self);
}
```

## TestDaemonConfig

```rust
/// Configuration for a test daemon instance.
pub struct TestDaemonConfig {
    /// Agent idle timeout. Default: Duration::from_secs(300).
    /// Set to Duration::from_millis(50) for lifecycle tests.
    pub idle_timeout: Duration,

    /// Rhai script for the agent's new_session_script.
    /// The agent runs this script for each new session.
    pub agent_script: String,

    /// Prior sessions known to the agent (for load/resume testing).
    pub prior_sessions: Vec<PriorSession>,
}

impl Default for TestDaemonConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(300),
            agent_script: String::new(),
            prior_sessions: Vec::new(),
        }
    }
}
```

## Behavioral Contract

1. `TestDaemon::start()` MUST:
   - Create a fresh temp directory
   - Start the daemon on an isolated socket within that directory
   - Wait until the socket file exists (max 2 seconds)
   - Configure the daemon with a `RhaiAgentFactory` using the provided config

2. `execute_client()` MUST:
   - Connect a `RhaiClient` to the daemon's socket
   - Execute the script to completion
   - Return the result string
   - Disconnect cleanly (the client connection closes)

3. Sequential `execute_client()` calls simulate disconnect/reconnect:
   - Each call is a fresh connection (new ACP client)
   - The test controls timing between calls (e.g., `tokio::time::sleep`)
   - The daemon sees each as a separate client arriving

4. On Drop:
   - The daemon task is aborted
   - All temp files are cleaned up
   - Works correctly even if the test panicked
