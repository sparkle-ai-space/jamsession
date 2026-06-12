use std::path::{Path, PathBuf};

use agent_client_protocol::{Agent, ByteStreams, Client, ConnectTo};
use tokio::net::UnixStream;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

pub struct UnixSocketTransport {
    socket_path: PathBuf,
}

impl UnixSocketTransport {
    pub fn new(socket_path: &Path) -> Self {
        Self {
            socket_path: socket_path.to_path_buf(),
        }
    }
}

impl ConnectTo<Client> for UnixSocketTransport {
    async fn connect_to(
        self,
        client: impl ConnectTo<Agent>,
    ) -> Result<(), agent_client_protocol::Error> {
        let stream = UnixStream::connect(&self.socket_path).await.map_err(|e| {
            agent_client_protocol::Error::new(-1, format!("socket connect failed: {e}"))
        })?;
        let (read_half, write_half) = stream.into_split();
        let transport = ByteStreams::new(write_half.compat_write(), read_half.compat());
        <ByteStreams<_, _> as ConnectTo<Client>>::connect_to(transport, client).await
    }
}
