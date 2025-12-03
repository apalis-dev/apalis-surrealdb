use apalis_core::{features_table, task::Task};
use apalis_sql::config::Config;

use std::marker::PhantomData;
use surrealdb::{Surreal, engine::any::Any};
use ulid::Ulid;

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
