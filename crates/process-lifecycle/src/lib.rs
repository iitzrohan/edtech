//! Supervised process cancellation, signal handling, and bounded graceful draining.
//!
//! This composition-support crate is the only place in Checkpoint 1 that starts Tokio tasks. It
//! contains no domain behavior and does not interpret cancellation as rollback of durable work.

use std::{collections::HashMap, future::Future, time::Duration};

use thiserror::Error;
use tokio::task::{Id, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::debug;

const MAX_TASK_NAME_LENGTH: usize = 96;

/// A provider-neutral failure returned by a required supervised task.
#[derive(Debug, Error)]
#[error("{message}")]
pub struct TaskFailure {
    message: String,
}

impl TaskFailure {
    /// Constructs a safe failure description for propagation to the composition root.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// The typed source of an orderly process shutdown request.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ShutdownReason {
    /// Unix SIGINT or the equivalent console interrupt.
    Interrupt,
    /// Unix SIGTERM.
    Terminate,
    /// An explicit in-process request, primarily useful at composition boundaries and in tests.
    Requested,
}

/// A lifecycle failure that prevents the process from claiming a clean shutdown.
#[derive(Debug, Error)]
pub enum LifecycleError {
    /// A task name is empty, unbounded, or contains control characters.
    #[error("supervised task name must contain 1 to 96 non-control bytes")]
    InvalidTaskName,
    /// New work was offered after root cancellation began.
    #[error("task supervisor is shutting down and cannot accept new work")]
    SupervisorShuttingDown,
    /// A named required task returned an error.
    #[error("required supervised task `{task}` failed")]
    TaskFailed {
        /// Fixed composition-edge task name.
        task: String,
        /// Typed task failure.
        #[source]
        source: TaskFailure,
    },
    /// A named task panicked or was unexpectedly cancelled outside orderly shutdown.
    #[error("required supervised task `{task}` terminated unexpectedly")]
    TaskJoinFailed {
        /// Fixed composition-edge task name.
        task: String,
    },
    /// Not every registered task terminated within the configured grace period.
    #[error("supervised tasks did not drain within {grace:?}")]
    ShutdownTimedOut {
        /// Configured bounded grace period.
        grace: Duration,
    },
    /// A supported operating-system signal stream could not be installed.
    #[error("shutdown signal handler could not be installed")]
    SignalRegistration(#[source] std::io::Error),
    /// The installed operating-system signal stream closed unexpectedly.
    #[error("shutdown signal stream closed unexpectedly")]
    SignalStreamClosed,
}

/// Owns every asynchronous task started by one process and its single root cancellation token.
pub struct TaskSupervisor {
    root: CancellationToken,
    tasks: JoinSet<Result<(), TaskFailure>>,
    names: HashMap<Id, String>,
}

impl TaskSupervisor {
    /// Creates a supervisor and the process's single root cancellation token.
    #[must_use]
    pub fn new() -> Self {
        Self {
            root: CancellationToken::new(),
            tasks: JoinSet::new(),
            names: HashMap::new(),
        }
    }

    /// Returns a child token that is cancelled whenever the process root is cancelled.
    #[must_use]
    pub fn child_token(&self) -> CancellationToken {
        self.root.child_token()
    }

    /// Returns the number of tasks that remain registered with this supervisor.
    #[must_use]
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    /// Starts and registers one named required task.
    ///
    /// # Errors
    ///
    /// Returns [`LifecycleError::InvalidTaskName`] before starting a task with an unsafe name.
    pub fn spawn<F>(&mut self, name: impl Into<String>, task: F) -> Result<(), LifecycleError>
    where
        F: Future<Output = Result<(), TaskFailure>> + Send + 'static,
    {
        if self.root.is_cancelled() {
            return Err(LifecycleError::SupervisorShuttingDown);
        }
        let name = name.into();
        if name.is_empty()
            || name.len() > MAX_TASK_NAME_LENGTH
            || name.chars().any(char::is_control)
        {
            return Err(LifecycleError::InvalidTaskName);
        }

        let handle = self.tasks.spawn(task);
        self.names.insert(handle.id(), name);
        Ok(())
    }

    /// Cancels the root, waits for every registered task, and enforces a finite grace period.
    ///
    /// A required task failure also cancels the root before sibling tasks are drained.
    ///
    /// # Errors
    ///
    /// Returns a [`LifecycleError`] for a failed task, task panic, shutdown-source failure, or
    /// bounded-drain timeout.
    pub async fn run_until_shutdown<S>(
        &mut self,
        shutdown: S,
        grace: Duration,
    ) -> Result<ShutdownReason, LifecycleError>
    where
        S: Future<Output = Result<ShutdownReason, LifecycleError>>,
    {
        tokio::pin!(shutdown);

        loop {
            if self.tasks.is_empty() {
                let reason = (&mut shutdown).await;
                return self.finish_shutdown(reason, grace).await;
            }

            tokio::select! {
                reason = &mut shutdown => {
                    return self.finish_shutdown(reason, grace).await;
                }
                joined = self.tasks.join_next_with_id() => {
                    match joined {
                        Some(Ok((id, Ok(())))) => {
                            self.remove_name(id);
                        }
                        Some(Ok((id, Err(source)))) => {
                            let task = self.remove_name(id);
                            self.root.cancel();
                            self.drain(grace).await?;
                            return Err(LifecycleError::TaskFailed { task, source });
                        }
                        Some(Err(join_error)) => {
                            let task = self.remove_name(join_error.id());
                            self.root.cancel();
                            self.drain(grace).await?;
                            return Err(LifecycleError::TaskJoinFailed { task });
                        }
                        None => {}
                    }
                }
            }
        }
    }

    async fn drain(&mut self, grace: Duration) -> Result<(), LifecycleError> {
        let drain = async {
            while let Some(joined) = self.tasks.join_next_with_id().await {
                match joined {
                    Ok((id, Ok(()))) => {
                        self.remove_name(id);
                    }
                    Ok((id, Err(source))) => {
                        let task = self.remove_name(id);
                        return Err(LifecycleError::TaskFailed { task, source });
                    }
                    Err(join_error) => {
                        let task = self.remove_name(join_error.id());
                        return Err(LifecycleError::TaskJoinFailed { task });
                    }
                }
            }
            Ok(())
        };

        if let Ok(result) = tokio::time::timeout(grace, drain).await {
            result
        } else {
            self.tasks.abort_all();
            while self.tasks.join_next().await.is_some() {}
            self.names.clear();
            Err(LifecycleError::ShutdownTimedOut { grace })
        }
    }

    async fn finish_shutdown(
        &mut self,
        reason: Result<ShutdownReason, LifecycleError>,
        grace: Duration,
    ) -> Result<ShutdownReason, LifecycleError> {
        self.root.cancel();
        self.drain(grace).await?;
        reason
    }

    fn remove_name(&mut self, id: Id) -> String {
        self.names
            .remove(&id)
            .unwrap_or_else(|| String::from("unknown-task"))
    }
}

impl Default for TaskSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TaskSupervisor {
    fn drop(&mut self) {
        self.root.cancel();
        self.tasks.abort_all();
        debug!(
            remaining_tasks = self.tasks.len(),
            "task supervisor dropped"
        );
    }
}

/// Waits for SIGINT or SIGTERM on Unix and for the console interrupt on other targets.
///
/// # Errors
///
/// Returns [`LifecycleError`] when signal registration fails or a signal stream closes.
#[cfg(unix)]
pub async fn shutdown_signal() -> Result<ShutdownReason, LifecycleError> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut interrupt =
        signal(SignalKind::interrupt()).map_err(LifecycleError::SignalRegistration)?;
    let mut terminate =
        signal(SignalKind::terminate()).map_err(LifecycleError::SignalRegistration)?;

    tokio::select! {
        received = interrupt.recv() => received
            .map(|()| ShutdownReason::Interrupt)
            .ok_or(LifecycleError::SignalStreamClosed),
        received = terminate.recv() => received
            .map(|()| ShutdownReason::Terminate)
            .ok_or(LifecycleError::SignalStreamClosed),
    }
}

/// Waits for the console interrupt on non-Unix targets.
///
/// # Errors
///
/// Returns [`LifecycleError`] when signal handling fails.
#[cfg(not(unix))]
pub async fn shutdown_signal() -> Result<ShutdownReason, LifecycleError> {
    tokio::signal::ctrl_c()
        .await
        .map(|()| ShutdownReason::Interrupt)
        .map_err(LifecycleError::SignalRegistration)
}

#[cfg(test)]
mod tests {
    use std::{future, time::Duration};

    use tokio::sync::oneshot;

    use super::{LifecycleError, ShutdownReason, TaskFailure, TaskSupervisor};

    const TEST_GRACE: Duration = Duration::from_secs(1);

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn root_cancellation_reaches_child_tasks_and_they_drain() {
        let mut supervisor = TaskSupervisor::new();
        let token = supervisor.child_token();
        let (started_tx, started_rx) = oneshot::channel();
        let (cancelled_tx, cancelled_rx) = oneshot::channel();

        assert!(
            supervisor
                .spawn("cancellation-observer", async move {
                    if started_tx.send(()).is_err() {
                        return Err(TaskFailure::new("test start receiver closed"));
                    }
                    token.cancelled().await;
                    if cancelled_tx.send(()).is_err() {
                        return Err(TaskFailure::new("test cancellation receiver closed"));
                    }
                    Ok(())
                })
                .is_ok()
        );

        let shutdown = async move {
            started_rx
                .await
                .map(|()| ShutdownReason::Requested)
                .map_err(|_| LifecycleError::SignalStreamClosed)
        };
        let result = supervisor.run_until_shutdown(shutdown, TEST_GRACE).await;

        assert!(matches!(result, Ok(ShutdownReason::Requested)));
        assert!(cancelled_rx.await.is_ok());
        assert_eq!(supervisor.task_count(), 0);
        assert!(matches!(
            supervisor.spawn("late-task", async { Ok(()) }),
            Err(LifecycleError::SupervisorShuttingDown)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn child_failure_cancels_and_drains_siblings() {
        let mut supervisor = TaskSupervisor::new();
        let sibling_token = supervisor.child_token();
        let (sibling_started_tx, sibling_started_rx) = oneshot::channel();
        let (sibling_cancelled_tx, sibling_cancelled_rx) = oneshot::channel();

        assert!(
            supervisor
                .spawn("sibling", async move {
                    if sibling_started_tx.send(()).is_err() {
                        return Err(TaskFailure::new("test start receiver closed"));
                    }
                    sibling_token.cancelled().await;
                    if sibling_cancelled_tx.send(()).is_err() {
                        return Err(TaskFailure::new("test cancellation receiver closed"));
                    }
                    Ok(())
                })
                .is_ok()
        );
        assert!(
            supervisor
                .spawn("failing-task", async move {
                    if sibling_started_rx.await.is_err() {
                        return Err(TaskFailure::new("sibling did not start"));
                    }
                    Err(TaskFailure::new("deliberate test failure"))
                })
                .is_ok()
        );

        let result = supervisor
            .run_until_shutdown(future::pending(), TEST_GRACE)
            .await;
        assert!(matches!(
            result,
            Err(LifecycleError::TaskFailed { task, .. }) if task == "failing-task"
        ));
        assert!(sibling_cancelled_rx.await.is_ok());
        assert_eq!(supervisor.task_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successful_tasks_are_joined_before_shutdown_completes() {
        let mut supervisor = TaskSupervisor::new();
        let (completed_tx, completed_rx) = oneshot::channel();
        assert!(
            supervisor
                .spawn("finite-task", async move {
                    if completed_tx.send(()).is_err() {
                        return Err(TaskFailure::new("test completion receiver closed"));
                    }
                    Ok(())
                })
                .is_ok()
        );

        let shutdown = async move {
            completed_rx
                .await
                .map(|()| ShutdownReason::Requested)
                .map_err(|_| LifecycleError::SignalStreamClosed)
        };
        let result = supervisor.run_until_shutdown(shutdown, TEST_GRACE).await;
        assert!(matches!(result, Ok(ShutdownReason::Requested)));
        assert_eq!(supervisor.task_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn non_terminating_task_causes_bounded_shutdown_failure() {
        let mut supervisor = TaskSupervisor::new();
        assert!(
            supervisor
                .spawn("non-terminating", async {
                    future::pending::<()>().await;
                    Ok(())
                })
                .is_ok()
        );

        let grace = Duration::from_millis(10);
        let result = supervisor
            .run_until_shutdown(future::ready(Ok(ShutdownReason::Requested)), grace)
            .await;
        assert!(matches!(
            result,
            Err(LifecycleError::ShutdownTimedOut { grace: actual }) if actual == grace
        ));
        assert_eq!(supervisor.task_count(), 0);
    }

    struct DropProbe(Option<oneshot::Sender<()>>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _send_result = sender.send(());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timed_out_task_is_aborted_and_not_silently_detached() {
        let mut supervisor = TaskSupervisor::new();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        assert!(
            supervisor
                .spawn("drop-observer", async move {
                    let _probe = DropProbe(Some(dropped_tx));
                    future::pending::<()>().await;
                    Ok(())
                })
                .is_ok()
        );

        let result = supervisor
            .run_until_shutdown(
                future::ready(Ok(ShutdownReason::Requested)),
                Duration::from_millis(10),
            )
            .await;
        assert!(matches!(
            result,
            Err(LifecycleError::ShutdownTimedOut { .. })
        ));
        assert!(dropped_rx.await.is_ok());
        assert_eq!(supervisor.task_count(), 0);
    }
}
