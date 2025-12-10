use apalis_core::{
    backend::{
        Backend, TaskStream,
        codec::{Codec, json::JsonCodec},
    },
    layers::Stack,
    task::Task,
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use apalis_sql::config::Config;
use futures::{
    FutureExt, Stream, StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use std::marker::PhantomData;
use surrealdb::{Surreal, engine::any::Any};
use ulid::Ulid;

use crate::{
    ack::{LockTaskLayer, SurrealAck},
    fetcher::{SurrealFetcher, SurrealPollFetcher},
    queries::{
        keep_alive::{initial_heartbeat, keep_alive_stream},
        reenqueue_orphaned::reenqueue_orphaned_stream,
    },
    sink::SurrealSink,
};

mod ack;
mod fetcher;
mod from_record;
/// Queries module for surrealdb backend
pub mod queries;
mod sink;

pub type SurrealContext = apalis_sql::context::SqlContext;

/// Type alias for a task stored in sqlite backend
pub type SurrealTask<Args> = Task<Args, SurrealContext, Ulid>;

/// CompactType is the type used for compact serialization in sqlite backend
pub type CompactType = Vec<u8>;

#[pin_project::pin_project]
pub struct SurrealStorage<T, C, Fetcher> {
    conn: Surreal<Any>,
    job_type: PhantomData<T>,
    codec: PhantomData<C>,
    config: Config,
    #[pin]
    sink: SurrealSink<T, CompactType, C>,
    #[pin]
    fetcher: Fetcher,
}

impl<T, C, F> std::fmt::Debug for SurrealStorage<T, C, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SurrealStorage")
            .field("conn", &self.conn)
            .field("job_type", &self.job_type)
            .field("codec", &std::any::type_name::<C>())
            .field("config", &self.config)
            .finish()
    }
}

impl<T, C, F: Clone> Clone for SurrealStorage<T, C, F> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            job_type: PhantomData,
            codec: self.codec,
            config: self.config.clone(),
            sink: self.sink.clone(),
            fetcher: self.fetcher.clone(),
        }
    }
}

pub fn query_file_as_str(path: &str) -> Result<String, Box<surrealdb::Error>> {
    let query = std::fs::read_to_string(path)
        .map_err(|e| surrealdb::Error::Db(surrealdb::error::Db::Thrown(e.to_string())))?;

    Ok(query)
}

impl SurrealStorage<(), (), ()> {
    /// Perform necessary setup
    pub async fn setup(conn: &Surreal<Any>) -> Result<(), surrealdb::Error> {
        let query = query_file_as_str("migrations/init_setup.surql").map_err(|e| *e)?;
        conn.use_ns("apalis").use_db("apalis").await?;
        conn.query(query).await?.check()?;
        Ok(())
    }
}

impl<T> SurrealStorage<T, (), ()> {
    /// Create a new SurrealStorage
    pub async fn new(
        db: &Surreal<Any>,
    ) -> Result<SurrealStorage<T, JsonCodec<CompactType>, SurrealFetcher>, surrealdb::Error> {
        let config = Config::new(std::any::type_name::<T>());
        db.use_ns("apalis").use_db("apalis").await?;

        Ok(SurrealStorage {
            conn: db.clone(),
            job_type: PhantomData,
            codec: PhantomData,
            sink: SurrealSink::new(db, &config),
            config,
            fetcher: SurrealFetcher,
        })
    }

    /// Create a new SurrealStorage with user-defined table name
    pub async fn new_with_queue(
        db: &Surreal<Any>,
        queue: &str,
    ) -> Result<SurrealStorage<T, JsonCodec<CompactType>, SurrealFetcher>, surrealdb::Error> {
        let config = Config::new(queue);
        db.use_ns("apalis").use_db("apalis").await?;
        Ok(SurrealStorage {
            conn: db.clone(),
            job_type: PhantomData,
            codec: PhantomData,
            sink: SurrealSink::new(db, &config),
            config,
            fetcher: SurrealFetcher,
        })
    }

    /// Create a new SurrealStorage woth config
    pub async fn new_with_config(
        db: &Surreal<Any>,
        config: &Config,
    ) -> Result<SurrealStorage<T, JsonCodec<CompactType>, SurrealFetcher>, surrealdb::Error> {
        db.use_ns("apalis").use_db("apalis").await?;
        Ok(SurrealStorage {
            conn: db.clone(),
            job_type: PhantomData,
            codec: PhantomData,
            config: config.clone(),
            sink: SurrealSink::new(db, config),
            fetcher: SurrealFetcher,
        })
    }
}

impl<T, C, F> SurrealStorage<T, C, F> {
    /// Change the codec used for serialization/desirialization
    pub fn with_codec<D>(self) -> SurrealStorage<T, D, F> {
        SurrealStorage {
            sink: SurrealSink::new(&self.conn, &self.config),
            conn: self.conn,
            job_type: PhantomData,
            codec: PhantomData,
            config: self.config,
            fetcher: self.fetcher,
        }
    }

    /// Get the config use by the storage
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Ge the connection used by the storage
    pub fn conn(&self) -> &Surreal<Any> {
        &self.conn
    }
}

impl<Args, Decode> Backend for SurrealStorage<Args, Decode, SurrealFetcher>
where
    Args: Send + Unpin + 'static,
    Decode: Codec<Args, Compact = CompactType> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;

    type IdType = Ulid;

    type Error = surrealdb::Error;

    type Context = SurrealContext;

    type Stream = TaskStream<SurrealTask<Args>, surrealdb::Error>;

    type Beat = BoxStream<'static, Result<(), surrealdb::Error>>;

    type Layer = Stack<LockTaskLayer, AcknowledgeLayer<SurrealAck>>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let conn = self.conn.clone();
        let config = self.config.clone();
        let worker = worker.clone();
        let keep_alive = keep_alive_stream(conn, config, worker);
        let reenqueue = reenqueue_orphaned_stream(
            self.conn.clone(),
            self.config.clone(),
            *self.config.keep_alive(),
        )
        .map_ok(|_| ());

        futures::stream::select(keep_alive, reenqueue).boxed()
    }

    fn middleware(&self) -> Self::Layer {
        let lock = LockTaskLayer::new(self.conn.clone());
        let ack = AcknowledgeLayer::new(SurrealAck::new(self.conn.clone()));
        Stack::new(lock, ack)
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        self.poll_default(worker)
            .map(|a| match a {
                Ok(Some(task)) => Ok(Some(task.try_map(|t| Decode::decode(&t)).map_err(|e| {
                    surrealdb::Error::Db(surrealdb::error::Db::Thrown(e.to_string()))
                })?)),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            })
            .boxed()
    }
}

impl<Args, Decode: Send + 'static, F> SurrealStorage<Args, Decode, F> {
    fn poll_default(
        self,
        worker: &WorkerContext,
    ) -> impl Stream<Item = Result<Option<SurrealTask<CompactType>>, surrealdb::Error>> + Send + 'static
    {
        let fut = initial_heartbeat(
            self.conn.clone(),
            self.config().clone(),
            worker.clone(),
            "SurrealStorage",
        );

        let register = stream::once(fut.map(|_| Ok(None)));
        register.chain(SurrealPollFetcher::<CompactType, Decode>::new(
            &self.conn,
            &self.config,
            worker,
        ))
    }
}

#[cfg(test)]
mod tests {
    use apalis::prelude::*;
    use apalis_sql::config::Config;
    use chrono::Local;
    use futures::{StreamExt, stream};

    // For an in memory database
    use surrealdb::engine::any::connect;

    use crate::{SurrealContext, SurrealStorage};

    #[tokio::test]
    async fn basic_worker() {
        const ITEMS: usize = 10;
        let db = connect("mem://").await.unwrap();

        SurrealStorage::setup(&db).await.unwrap();

        let config = Config::new("basic-worker-queue");

        let backend = SurrealStorage::new_with_config(&db, &config).await.unwrap();

        let worker_context = WorkerContext::new::<fn(usize, WorkerContext)>("rango-tango-1");

        crate::queries::keep_alive::initial_heartbeat(
            db.clone(),
            config.clone(),
            worker_context.clone(),
            "SurrealStorage",
        )
        .await
        .unwrap();

        let mut start = 0;
        let items = stream::repeat_with(move || {
            start += 1;
            Task::builder(serde_json::to_vec(&start).unwrap())
                .with_ctx(SurrealContext::new().with_priority(start))
                .build()
        })
        .take(ITEMS)
        .collect::<Vec<_>>()
        .await;

        crate::sink::push_tasks(db.clone(), config, items)
            .await
            .unwrap();

        println!("Start worker at {}", Local::now());

        async fn send_reminder(item: usize, wrk: WorkerContext) -> Result<(), BoxDynError> {
            if ITEMS == item {
                wrk.stop().unwrap();
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango-1")
            .backend(backend)
            .build(send_reminder);

        worker
            .run_with_ctx(&mut worker_context.clone())
            .await
            .unwrap();
    }
}
