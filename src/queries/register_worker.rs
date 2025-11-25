use apalis_core::worker::context::WorkerContext;
use apalis_sql::config::Config;
use std::io;
use surrealdb::{Surreal, engine::any::Any};

use crate::query_file_as_str;

/// Register a new worker in the database
pub async fn register_worker(
    conn: Surreal<Any>,
    config: Config,
    worker: WorkerContext,
    storage_type: &str,
) -> Result<(), surrealdb::Error> {
    let worker_id = worker.name().to_owned();
    let queue = config.queue().to_string();
    let layers = worker.get_service().to_owned();
    let keep_alive = config.keep_alive().as_secs() as i64;
    let storage_name = storage_type.to_string();

    let query = query_file_as_str("queries/backend/register_worker.surql").map_err(|e| *e)?;

    let mut res = conn
        .query(query)
        .bind(("worker_id", worker_id))
        .bind(("queue", queue))
        .bind(("storage_name", storage_name))
        .bind(("layers", layers))
        .bind(("keep_alive", keep_alive))
        .await?;

    let res: Option<u64> = res.take(2)?;

    if res.is_none() || res.map(|r| r == 0).unwrap_or(false) {
        return Err(surrealdb::Error::Db(surrealdb::error::Db::Io(
            io::Error::new(io::ErrorKind::AlreadyExists, "WORKER_ALREADY_EXISTS"),
        )));
    }

    Ok(())
}
