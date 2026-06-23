use std::pin::Pin;
use std::task::{Context, Poll};

use agent_client_protocol::{BoxFuture, Channel, ConnectTo};
use futures::stream::Stream;
use tokio::sync::oneshot;

/// A transport wrapper that signals when the incoming stream ends.
///
/// Workaround for <https://github.com/agentclientprotocol/rust-sdk/issues/223>:
/// `connect_with` never returns when the remote end closes because the SDK's
/// internal `try_join!` waits for all actors, and the outgoing actor blocks on
/// its channel forever even after incoming EOF.
///
/// This wrapper intercepts the `Channel.rx` stream and fires a oneshot when
/// the stream yields `None` (connection closed). The caller can use this signal
/// to break out of any outgoing loop via `take_until`. Remove this once the
/// upstream issue is fixed.
pub(crate) struct EofSignalingTransport<T> {
    inner: T,
    close_tx: oneshot::Sender<()>,
}

impl<T> EofSignalingTransport<T> {
    pub(crate) fn wrap(inner: T) -> (Self, oneshot::Receiver<()>) {
        let (close_tx, close_rx) = oneshot::channel();
        (Self { inner, close_tx }, close_rx)
    }
}

impl<T, R> ConnectTo<R> for EofSignalingTransport<T>
where
    T: ConnectTo<R>,
    R: agent_client_protocol::role::Role,
{
    async fn connect_to(
        self,
        client: impl ConnectTo<R::Counterpart>,
    ) -> agent_client_protocol::schema::Result<()> {
        let (channel, future) = ConnectTo::<R>::into_channel_and_future(self);
        match futures::future::select(Box::pin(client.connect_to(channel)), future).await {
            futures::future::Either::Left((result, _))
            | futures::future::Either::Right((result, _)) => result,
        }
    }

    fn into_channel_and_future(
        self,
    ) -> (
        Channel,
        BoxFuture<'static, agent_client_protocol::schema::Result<()>>,
    ) {
        let (channel, future) = self.inner.into_channel_and_future();
        let wrapped_rx = EofSignalingStream::new(channel.rx, self.close_tx);
        let forwarding_rx = wrapped_rx.into_forwarding_receiver();
        let wrapped_channel = Channel {
            rx: forwarding_rx,
            tx: channel.tx,
        };
        (wrapped_channel, future)
    }
}

type ChannelItem = Result<agent_client_protocol::jsonrpcmsg::Message, agent_client_protocol::Error>;

/// A stream wrapper that fires a oneshot when the inner stream ends.
struct EofSignalingStream {
    inner: futures::channel::mpsc::UnboundedReceiver<ChannelItem>,
    close_tx: Option<oneshot::Sender<()>>,
}

impl EofSignalingStream {
    fn new(
        inner: futures::channel::mpsc::UnboundedReceiver<ChannelItem>,
        close_tx: oneshot::Sender<()>,
    ) -> Self {
        Self {
            inner,
            close_tx: Some(close_tx),
        }
    }

    /// Spawn a task that forwards items from this stream into a new channel,
    /// preserving the EOF signal behavior.
    fn into_forwarding_receiver(self) -> futures::channel::mpsc::UnboundedReceiver<ChannelItem> {
        let (tx, rx) = futures::channel::mpsc::unbounded();
        tokio::spawn(async move {
            use futures::StreamExt;
            let mut stream = self;
            while let Some(item) = stream.next().await {
                if tx.unbounded_send(item).is_err() {
                    break;
                }
            }
        });
        rx
    }
}

impl Stream for EofSignalingStream {
    type Item = ChannelItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = Pin::new(&mut self.inner).poll_next(cx);
        if matches!(result, Poll::Ready(None)) {
            let _ = self.close_tx.take().map(|tx| tx.send(()));
        }
        result
    }
}
