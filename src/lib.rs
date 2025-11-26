use apalis_core::{
    backend::{
        Backend, BackendExt, TaskStream,
        codec::{Codec, json::JsonCodec},
    },
    features_table,
    layers::Identity,
    task::Task,
    worker::context::WorkerContext,
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
    fetcher::{SurrealFetcher, SurrealPollFetcher},
    queries::{
        keep_alive::{initial_heartbeat, keep_alive_stream},
        reenqueue_orphaned::reenqueue_orphaned_stream,
    },
    sink::SurrealSink,
};

mod config;
/// Fetcher module for retrieving tasks from surrealdb backend
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

#[doc = features_table! {
    setup = r#"
        #   {
        #   use apalis_surrealdb::SurrealStorage;
        #   use surrealdb::engine::any::connect;
        #   let db = connect("mem://").await.unwrap();
        #   SurrealStorage::setup(&db).await.unwrap();
        #   SurrealStorage::new(&db).await.unwrap(); 
        # };
    "#,
    Backend => supported("Supports storage and retrieval of tasks", true),
    TaskSink => supported("Ability to push new tasks", true),
    Serialization => supported("Serialization support for arguments", true),
    Workflow => supported("Flexible enough to support workflows", true),
    WebUI => supported("Expose a web interface for monitoring tasks", true),
    FetchById => supported("Allow fetching a task by its ID", false),
    RegisterWorker => supported("Allow registering a worker with the backend", false),
    MakeShared => supported("Share one connection across multiple workers via [`SharedSqliteStorage`]", false),
    WaitForCompletion => supported("Wait for tasks to complete without blocking", true),
    ResumeById => supported("Resume a task by its ID", false),
    ResumeAbandoned => supported("Resume abandoned tasks", false),
    ListWorkers => supported("List all workers registered with the backend", false),
    ListTasks => supported("List all tasks in the backend", false),
}]
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

    type Layer = Identity;

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
        Identity::new()
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

impl<Args, Decode: Send + 'static> BackendExt for SurrealStorage<Args, Decode, SurrealFetcher>
where
    Self: Backend<Args = Args, IdType = Ulid, Context = SurrealContext, Error = surrealdb::Error>,
    Decode: Codec<Args, Compact = CompactType> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
    Args: Send + Unpin + 'static,
{
    type Codec = Decode;
    type Compact = CompactType;
    type CompactStream = TaskStream<SurrealTask<Self::Compact>, surrealdb::Error>;

    fn poll_compact(self, worker: &WorkerContext) -> Self::CompactStream {
        self.poll_default(worker).boxed()
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
    use std::time::Duration;

    use apalis::prelude::*;
    use apalis_sql::config::Config;
    use apalis_workflow::*;
    use chrono::Local;
    use futures::{StreamExt, stream};
    use serde::{Deserialize, Serialize};

    // For an in memory database
    use surrealdb::engine::any::connect;

    use crate::SurrealStorage;

    use surrealdb::opt::auth::Root;

    #[tokio::test]
    async fn basic_worker() {
        const ITEMS: usize = 10;
        let db = connect("mem://").await.unwrap();
        SurrealStorage::setup(&db).await.unwrap();

        let mut backend = SurrealStorage::new(&db).await.unwrap();

        let mut start = 0;

        let mut items = stream::repeat_with(move || {
            start += 1;
            start
        })
        .take(ITEMS);
        backend.push_stream(&mut items).await.unwrap();

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

        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn test_odd_workflow() {
        let workerflow = Workflow::new("odd-numbers-workflow")
            .and_then(|a: usize| async move { Ok::<_, BoxDynError>((0..=a).collect::<Vec<_>>()) })
            .filter_map(|x| async move { if x % 2 != 0 { Some(x) } else { None } })
            .filter_map(|x| async move { if x % 2 != 0 { Some(x) } else { None } })
            .filter_map(|x| async move { if x % 5 != 0 { Some(x) } else { None } })
            .delay_for(Duration::from_millis(1000))
            .and_then(|a: Vec<usize>| async move {
                println!("Sum: {}", a.iter().sum::<usize>());

                Err::<(), BoxDynError>("Intentional Error".into())
            });

        let db = connect("ws://localhost:5439").await.unwrap();
        db.signin(Root {
            username: "main",
            password: "main123",
        })
        .await
        .unwrap();

        SurrealStorage::setup(&db).await.unwrap();

        let mut backend = SurrealStorage::new(&db).await.unwrap();

        backend.push_start(100usize).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                println!("On Event = {:?}", ev);

                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build(workerflow);

        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn test_workflow_complete() {
        #[derive(Debug, Serialize, Deserialize, Clone)]
        struct PipelineConfig {
            min_confidence: f32,
            enable_sentiment: bool,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct UserInput {
            text: String,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct Classified {
            text: String,
            label: String,
            confidence: f32,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct Summary {
            text: String,
            sentiment: Option<String>,
        }

        let workflow = Workflow::new("text-pipeline")
            // Step 1: Preprocess input (e.g., tokenize, lowercase)
            .and_then(|input: UserInput, mut worker: WorkerContext| async move {
                worker.emit(&Event::custom(format!(
                    "Preprocessing input: {}",
                    input.text
                )));

                let processed = input.text.to_lowercase();
                Ok::<_, BoxDynError>(processed)
            })
            // Step 2: Classify text
            .and_then(|text: String| async move {
                let confidence = 0.85; // pretend model confidence
                let items = text.split_whitespace().collect::<Vec<_>>();
                let results = items
                    .into_iter()
                    .map(|x| Classified {
                        text: x.to_string(),
                        label: if x.contains("rust") {
                            "Tech"
                        } else {
                            "General"
                        }
                        .to_string(),
                        confidence,
                    })
                    .collect::<Vec<_>>();
                Ok::<_, BoxDynError>(results)
            })
            // Step 3: Filter out low-confidence predictions
            .filter_map(
                |c: Classified| async move { if c.confidence >= 0.6 { Some(c) } else { None } },
            )
            .filter_map(move |c: Classified, config: Data<PipelineConfig>| {
                let cfg = config.enable_sentiment;
                async move {
                    if !cfg {
                        return Some(Summary {
                            text: c.text,
                            sentiment: None,
                        });
                    }

                    // Sentiment model inference assumption
                    let sentiment = if c.text.contains("delightful") {
                        "positive"
                    } else {
                        "neutral"
                    };

                    Some(Summary {
                        text: c.text,
                        sentiment: Some(sentiment.to_string()),
                    })
                }
            })
            .and_then(|a: Vec<Summary>, mut worker: WorkerContext| async move {
                worker.emit(&Event::Custom(Box::new(format!(
                    "Generated {} summaries",
                    a.len()
                ))));
                worker.stop()
            });

        let db = connect("mem://").await.unwrap();
        SurrealStorage::setup(&db).await.unwrap();

        let config = Config::new("workflow-queue").with_poll_interval(
            StrategyBuilder::new()
                .apply(IntervalStrategy::new(Duration::from_millis(100)))
                .build(),
        );

        let mut backend = SurrealStorage::new_with_config(&db, &config).await.unwrap();

        let input = UserInput {
            text: "Rust makes systems programming delightful!".to_string(),
        };
        backend.push_start(input).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .data(PipelineConfig {
                min_confidence: 0.8,
                enable_sentiment: true,
            })
            .on_event(|ctx, ev| match ev {
                Event::Custom(msg) => {
                    if let Some(m) = msg.downcast_ref::<String>() {
                        println!("Custom Message: {}", m);
                    }
                }

                Event::Error(_) => {
                    println!("On Error = {:?}", ev);
                    ctx.stop().unwrap();
                }

                _ => {
                    println!("On Event = {:?}", ev)
                }
            })
            .build(workflow);

        worker.run().await.unwrap();
    }
}
