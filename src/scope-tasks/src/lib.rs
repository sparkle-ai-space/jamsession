use std::future::Future;
use std::pin::Pin;

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

type BoxFuture<'scope, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'scope>>;

/// Handle for submitting tasks into a scope. Clone-able, Send-able.
///
/// The lifetime `'scope` ties spawned futures to the enclosing scope —
/// they can borrow from the caller's stack frame.
///
/// Generic over error type `E` so callers choose their own error domain.
pub struct TaskSpawner<'scope, E> {
    tx: mpsc::UnboundedSender<BoxFuture<'scope, E>>,
}

impl<E> Clone for TaskSpawner<'_, E> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

/// Trait for constructing an error when `spawn` is called on a closed scope.
pub trait SpawnError: Send {
    fn scope_closed() -> Self;
}

impl<'scope, E: SpawnError> TaskSpawner<'scope, E> {
    /// Spawn a task to run concurrently in the scope's background.
    ///
    /// Returns `Err` if the scope has already shut down.
    pub fn spawn(&self, fut: impl Future<Output = Result<(), E>> + Send + 'scope) -> Result<(), E> {
        self.tx.send(Box::pin(fut)).map_err(|_| E::scope_closed())
    }
}

/// This macro is used for the `hack` parameter of [`scope`].
///
/// It expands to `|f, t| Box::pin(f(t))`.
///
/// This is needed until [return-type notation](https://github.com/rust-lang/rust/issues/109417)
/// is stabilized.
#[macro_export]
macro_rules! scope_hack {
    () => {
        |f, t| Box::pin(f(t))
    };
}

/// Run `main_fn` as the foreground, with a concurrent task runner in the background.
///
/// `main_fn` receives a `TaskSpawner` it can use to kick off concurrent work.
/// When `main_fn` returns, in-flight background tasks are cancelled (dropped)
/// and `scope` returns the foreground's result.
///
/// If any background task returns `Err`, the error propagates and the scope exits.
///
/// The `hack` parameter works around a compiler limitation: async closures
/// don't reliably prove `Send` across higher-ranked lifetimes. The caller
/// manually boxes the call, giving the compiler the proof it needs:
///
/// ```ignore
/// scope(
///     async |tasks| { /* ... */ },
///     scope_hack!(),
/// ).await
/// ```
pub async fn scope<'env, T, E, F>(
    main_fn: F,
    hack: impl FnOnce(
        F,
        TaskSpawner<'env, E>,
    ) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'env>>,
) -> Result<T, E>
where
    T: 'env,
    E: Send + 'env,
    F: AsyncFnOnce(TaskSpawner<'env, E>) -> Result<T, E> + 'env,
{
    let (tx, rx) = mpsc::unbounded_channel();
    let spawner = TaskSpawner { tx };

    run_until(run_tasks(rx), hack(main_fn, spawner)).await
}

/// Background runner: receives tasks from the channel and runs them concurrently
/// in a FuturesUnordered. Exits when the channel closes and all tasks complete,
/// or is cancelled by `run_until` when the foreground finishes first.
async fn run_tasks<E>(rx: mpsc::UnboundedReceiver<BoxFuture<'_, E>>) -> Result<(), E> {
    let mut stream = UnboundedReceiverStream::new(rx);
    let mut tasks: FuturesUnordered<BoxFuture<'_, E>> = FuturesUnordered::new();
    let mut stream_done = false;

    loop {
        if tasks.is_empty() && stream_done {
            return Ok(());
        }

        if tasks.is_empty() {
            match stream.next().await {
                Some(task) => {
                    tasks.push(task);
                    continue;
                }
                None => return Ok(()),
            }
        }

        if stream_done {
            while let Some(result) = tasks.next().await {
                result?;
            }
            return Ok(());
        }

        tokio::select! {
            biased;

            maybe_task = stream.next() => {
                match maybe_task {
                    Some(task) => tasks.push(task),
                    None => stream_done = true,
                }
            }

            maybe_result = tasks.next() => {
                if let Some(result) = maybe_result {
                    result?;
                }
            }
        }
    }
}

/// Race foreground against background. When foreground completes, drop background
/// (cancelling it). If background errors first, propagate that error.
async fn run_until<T, E>(
    background: impl Future<Output = Result<(), E>>,
    foreground: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    tokio::pin!(background);
    tokio::pin!(foreground);

    tokio::select! {
        biased;

        result = &mut foreground => result,
        result = &mut background => {
            match result {
                Err(e) => Err(e),
                Ok(()) => foreground.await,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    #[derive(Debug, PartialEq)]
    enum TestError {
        ScopeClosed,
        Task(String),
    }

    impl SpawnError for TestError {
        fn scope_closed() -> Self {
            Self::ScopeClosed
        }
    }

    #[tokio::test]
    async fn tasks_run_concurrently() {
        let counter = AtomicUsize::new(0);

        let result: Result<(), TestError> = scope(
            async |spawner| {
                spawner.spawn(async {
                    counter.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                })?;
                spawner.spawn(async {
                    counter.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                })?;

                tokio::task::yield_now().await;
                tokio::task::yield_now().await;

                assert_eq!(counter.load(Ordering::Relaxed), 2);
                Ok(())
            },
            scope_hack!(),
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn foreground_exit_cancels_inflight_tasks() {
        let counter = AtomicUsize::new(0);

        let result: Result<&str, TestError> = scope(
            async |spawner| {
                spawner.spawn(async {
                    loop {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                })?;

                tokio::time::sleep(Duration::from_millis(30)).await;
                Ok("done")
            },
            scope_hack!(),
        )
        .await;

        assert_eq!(result.unwrap(), "done");
        let final_count = counter.load(Ordering::Relaxed);

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            counter.load(Ordering::Relaxed),
            final_count,
            "task continued after scope exited"
        );
    }

    #[tokio::test]
    async fn task_error_propagates() {
        let result: Result<(), TestError> = scope(
            async |spawner| {
                spawner.spawn(async { Err(TestError::Task("boom".into())) })?;

                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(())
            },
            scope_hack!(),
        )
        .await;

        assert_eq!(result, Err(TestError::Task("boom".into())));
    }

    #[tokio::test]
    async fn spawn_on_closed_scope_returns_error() {
        let captured_spawner: Arc<std::sync::Mutex<Option<TaskSpawner<'static, TestError>>>> =
            Default::default();

        let _: Result<(), TestError> = scope(
            {
                let captured_spawner = captured_spawner.clone();
                async move |spawner| {
                    *captured_spawner.lock().unwrap() = Some(spawner.clone());
                    Ok(())
                }
            },
            scope_hack!(),
        )
        .await;

        let spawner = captured_spawner.lock().unwrap().take().unwrap();
        let err = spawner.spawn(async { Ok(()) }).unwrap_err();
        assert_eq!(err, TestError::ScopeClosed);
    }

    #[tokio::test]
    async fn non_static_borrows_compile() {
        let data = vec![1u32, 2, 3];
        let sum = AtomicUsize::new(0);

        let result: Result<usize, TestError> = scope(
            async |spawner| {
                let sum = &sum;
                for item in &data {
                    let item = *item;
                    spawner.spawn(async move {
                        sum.fetch_add(item as usize, Ordering::Relaxed);
                        Ok(())
                    })?;
                }
                tokio::task::yield_now().await;
                tokio::task::yield_now().await;
                Ok(sum.load(Ordering::Relaxed))
            },
            scope_hack!(),
        )
        .await;

        assert_eq!(result.unwrap(), 6);
    }

    #[tokio::test]
    async fn many_tasks_complete() {
        let counter = AtomicUsize::new(0);

        let result: Result<usize, TestError> = scope(
            async |spawner| {
                for _ in 0..100 {
                    spawner.spawn(async {
                        counter.fetch_add(1, Ordering::Relaxed);
                        Ok(())
                    })?;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(counter.load(Ordering::Relaxed))
            },
            scope_hack!(),
        )
        .await;

        assert_eq!(result.unwrap(), 100);
    }
}
