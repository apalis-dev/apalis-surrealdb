use apalis_core::task::Task;
use apalis_sql::config::Config;

use std::marker::PhantomData;
use surrealdb::{Surreal, engine::any::Any};
use ulid::Ulid;

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
