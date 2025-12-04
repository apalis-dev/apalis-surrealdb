use std::sync::Arc;
use std::{marker::PhantomData, pin::Pin, task::Poll};

use apalis_sql::config::Config;
use futures::FutureExt;
use futures::{
    Sink,
    future::{BoxFuture, Shared},
};
use surrealdb::{RecordId, Surreal, engine::any::Any, sql::Bytes};
use ulid::Ulid;

use crate::from_record::RawSurrealTask;
use crate::{CompactType, SurrealStorage, SurrealTask};

type FlushFuture = BoxFuture<'static, Result<(), Arc<surrealdb::Error>>>;

/// Sink for pushing tasks into the SurrealDB backend
#[pin_project::pin_project]
#[derive(Debug)]
pub(crate) struct SurrealSink<Args, Compact, Codec> {
    conn: Surreal<Any>,
    config: Config,
    buffer: Vec<SurrealTask<Compact>>,
    flush_future: Option<Shared<FlushFuture>>,
    _marker: PhantomData<(Args, Codec)>,
}

impl<Args, Compact, Codec> Clone for SurrealSink<Args, Compact, Codec> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            config: self.config.clone(),
            buffer: Vec::new(),
            flush_future: None,
            _marker: PhantomData,
        }
    }
}

/// Push a batch of tasks into the database
pub async fn push_tasks(
    conn: Surreal<Any>,
    cfg: Config,
    buffer: Vec<SurrealTask<CompactType>>,
) -> Result<(), Arc<surrealdb::Error>> {
    let tasks: Vec<RawSurrealTask> = buffer
        .into_iter()
        .map(|task| {
            let id = task
                .parts
                .task_id
                .map(|id| id.to_string())
                .unwrap_or(Ulid::new().to_string());
            let run_at = task.parts.run_at as i64;
            let max_attempts = task.parts.ctx.max_attempts();
            let priority = task.parts.ctx.priority();
            let args = task.args;

            // Using specified queue if specified, otherwise use default
            let job_type = cfg.queue().to_string();
            let metadata = serde_json::Value::Object(task.parts.ctx.meta().clone());

            RawSurrealTask {
                id: Some(RecordId::from_table_key("jobs", id)),
                job: Bytes::from(args),
                job_type: Some(job_type),
                status: Some("Pending".into()),
                attempts: Some(0),
                max_attempts: Some(max_attempts),
                run_at: Some(run_at),
                last_result: None,
                lock_at: None,
                lock_by: None,
                done_at: None,
                priority: Some(priority),
                metadata: Some(metadata),
            }
        })
        .collect();

    let _: Vec<RawSurrealTask> = conn.insert("jobs").content(tasks).await?;

    Ok(())
}

impl<Args, Compact, Codec> SurrealSink<Args, Compact, Codec> {
    /// Create new SurrealSink
    #[must_use]
    pub(crate) fn new(conn: &Surreal<Any>, config: &Config) -> Self {
        Self {
            conn: conn.clone(),
            config: config.clone(),
            buffer: Vec::new(),
            flush_future: None,
            _marker: PhantomData,
        }
    }
}

impl<Args, Encode, Fetcher> Sink<SurrealTask<CompactType>> for SurrealStorage<Args, Encode, Fetcher>
where
    Args: Send + Sync + 'static,
{
    type Error = surrealdb::Error;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: SurrealTask<CompactType>) -> Result<(), Self::Error> {
        // Add the item to the buffer
        self.project().sink.buffer.push(item);
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();

        // If the there's no existing future and buffer
        if this.sink.flush_future.is_none() && this.sink.buffer.is_empty() {
            return Poll::Ready(Ok(()));
        }

        // Create the future only if we don't have one and there's work to do
        if this.sink.flush_future.is_none() && !this.sink.buffer.is_empty() {
            let conn = this.conn.clone();
            let config = this.config.clone();
            let buffer = std::mem::take(&mut this.sink.buffer);
            let sink_fut = push_tasks(conn, config, buffer);
            this.sink.flush_future = Some((Box::pin(sink_fut) as FlushFuture).shared());
        }

        // Poll the existing future
        if let Some(mut fut) = this.sink.flush_future.take() {
            match fut.poll_unpin(cx) {
                Poll::Ready(Ok(())) => {
                    // Future completed successfully, don't put it back
                    Poll::Ready(Ok(()))
                }

                Poll::Ready(Err(e)) => {
                    // Future completed with error, don't put it back
                    Poll::Ready(Err(Arc::into_inner(e).unwrap()))
                }

                Poll::Pending => {
                    // Fiture is still pending, put it back and return pending
                    this.sink.flush_future = Some(fut);
                    Poll::Pending
                }
            }
        } else {
            // No fiture and no job to do/run
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}
