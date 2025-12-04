use std::{
    collections::VecDeque,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, atomic::AtomicUsize},
    task::Poll,
};

use apalis_core::{
    backend::poll_strategy::{PollContext, PollStrategyExt},
    task::Task,
    worker::context::WorkerContext,
};
use apalis_sql::{config::Config, from_row::TaskRow};
use futures::{FutureExt, Stream, future::BoxFuture};
use surrealdb::{RecordId, Surreal, engine::any::Any};
use ulid::Ulid;

use crate::{
    CompactType, SurrealContext, SurrealTask,
    from_record::{RawSurrealTask, SurrealTaskRecord},
    query_file_as_str,
};

/// Fetch the next batch of tasks from the sqlite backend
pub(crate) async fn fetch_next(
    conn: Surreal<Any>,
    config: Config,
    worker: WorkerContext,
) -> Result<Vec<Task<CompactType, SurrealContext, Ulid>>, surrealdb::Error> {
    let job_type = config.queue().to_string();
    let buffer_size = config.buffer_size() as i32;
    let worker = worker.name().clone();

    let query = query_file_as_str("queries/backend/fetch_next.surql").map_err(|e| *e)?;

    let mut result = conn
        .query(query)
        .bind(("worker_id", RecordId::from_table_key("workers", worker)))
        .bind(("v_job_type", job_type))
        .bind(("v_job_count", buffer_size))
        .await?;

    let tasks: Vec<RawSurrealTask> = result.take(1)?;

    let tasks: Result<Vec<_>, _> = tasks
        .into_iter()
        .map(|r| {
            let r: SurrealTaskRecord = r.into();
            let row: TaskRow = r.try_into()?;

            row.try_into_task_compact::<Ulid>()
                .map_err(|e| surrealdb::Error::Db(surrealdb::error::Db::Thrown(e.to_string())))
        })
        .collect();

    let tasks = tasks?;

    Ok(tasks)
}

enum StreamState {
    Ready,
    Delay,
    Fetch(BoxFuture<'static, Result<Vec<SurrealTask<CompactType>>, surrealdb::Error>>),
    Buffered(VecDeque<SurrealTask<CompactType>>),
    Empty,
}

/// Dispatcher for fetching tasks from a SurrealDb store backend via [SurrealPollFetcher]
#[derive(Clone, Debug)]
pub struct SurrealFetcher;

/// Polling-based fetcher for retrieving tasks from a SurreaDb store backend
#[pin_project::pin_project]
pub(crate) struct SurrealPollFetcher<Compact, Decode> {
    conn: Surreal<Any>,
    config: Config,
    wrk: WorkerContext,
    _marker: PhantomData<(Compact, Decode)>,
    #[pin]
    state: StreamState,
    #[pin]
    delay_stream: Option<Pin<Box<dyn Stream<Item = ()> + Send>>>,
    prev_count: Arc<AtomicUsize>,
}

impl<Compact, Decode> std::fmt::Debug for SurrealPollFetcher<Compact, Decode> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SurrealPollFetcher")
            .field("conn", &self.conn)
            .field("config", &self.config)
            .field("wrk", &self.wrk)
            .field("_marker", &self._marker)
            .field("prev_count", &self.prev_count)
            .finish()
    }
}

impl<Compact, Decode> Clone for SurrealPollFetcher<Compact, Decode> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            config: self.config.clone(),
            wrk: self.wrk.clone(),
            _marker: PhantomData,
            state: StreamState::Ready,
            delay_stream: None,
            prev_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl<Decode> SurrealPollFetcher<CompactType, Decode> {
    /// Create an new SurrealPollFetcher
    #[must_use]
    pub(crate) fn new(conn: &Surreal<Any>, config: &Config, wrk: &WorkerContext) -> Self {
        Self {
            conn: conn.clone(),
            config: config.clone(),
            wrk: wrk.clone(),
            _marker: PhantomData,
            state: StreamState::Ready,
            delay_stream: None,
            prev_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl<Decode> Stream for SurrealPollFetcher<CompactType, Decode> {
    type Item = Result<Option<SurrealTask<CompactType>>, surrealdb::Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.delay_stream.is_none() {
            let strategy = this
                .config
                .poll_strategy()
                .clone()
                .build_stream(&PollContext::new(this.wrk.clone(), this.prev_count.clone()));

            this.delay_stream = Some(Box::pin(strategy));
        }

        loop {
            match this.state {
                StreamState::Ready => {
                    let stream =
                        fetch_next(this.conn.clone(), this.config.clone(), this.wrk.clone());
                    this.state = StreamState::Fetch(stream.boxed());
                }
                StreamState::Delay => {
                    if let Some(delay_stream) = this.delay_stream.as_mut() {
                        match delay_stream.as_mut().poll_next(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(Some(_)) => {
                                this.state = StreamState::Ready;
                            }
                            Poll::Ready(None) => {
                                this.state = StreamState::Empty;
                                return Poll::Ready(None);
                            }
                        }
                    } else {
                        this.state = StreamState::Empty;
                        return Poll::Ready(None);
                    }
                }

                StreamState::Fetch(ref mut fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(item) => match item {
                        Ok(requests) => {
                            if requests.is_empty() {
                                this.state = StreamState::Delay;
                            } else {
                                let mut buffer = VecDeque::new();
                                for request in requests {
                                    buffer.push_back(request);
                                }

                                this.state = StreamState::Buffered(buffer);
                            }
                        }
                        Err(e) => {
                            this.state = StreamState::Empty;
                            return Poll::Ready(Some(Err(e)));
                        }
                    },
                },

                StreamState::Buffered(ref mut buffer) => {
                    if let Some(request) = buffer.pop_front() {
                        // Yield the next buffered item
                        if buffer.is_empty() {
                            // Buffer is now empty, transition to ready for next fetch
                            this.state = StreamState::Ready;
                        }

                        return Poll::Ready(Some(Ok(Some(request))));
                    } else {
                        // Buffer is empty, transition to ready
                        this.state = StreamState::Ready;
                    }
                }

                StreamState::Empty => return Poll::Ready(None),
            }
        }
    }
}

impl<Compact, Decode> SurrealPollFetcher<Compact, Decode> {
    /// Take pending tasks from the fetcher
    pub(crate) fn _take_pending(&mut self) -> VecDeque<SurrealTask<Vec<u8>>> {
        match &mut self.state {
            StreamState::Buffered(tasks) => std::mem::take(tasks),
            _ => VecDeque::new(),
        }
    }
}
