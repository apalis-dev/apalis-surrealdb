use apalis_core::worker::context::WorkerContext;
use apalis_sql::config::Config;
use futures::{FutureExt, Stream, stream};
use std::io;
use surrealdb::{RecordId, Surreal, engine::any::Any};

use crate::{
    queries::{reenqueue_orphaned::reenqueue_orphaned, register_worker::register_worker},
    query_file_as_str,
};

/// Send a keep-alive signal to the database to indicate that the worker is still active
pub async fn keep_alive(
    conn: Surreal<Any>,
    config: Config,
    worker: WorkerContext,
) -> Result<(), surrealdb::Error> {
    let worker_id = worker.name().to_owned();
    let queue = config.queue().to_string();

    let query = query_file_as_str("queries/backend/keep_alive.surql").map_err(|e| *e)?;

    let mut res = conn
        .query(query)
        .bind(("worker_id", RecordId::from_table_key("workers", worker_id)))
        .bind(("queue", queue))
        .await?;

    let res: Option<i32> = res.take(1)?;

    if res.is_none() || res.map(|r| r == 0).unwrap_or(false) {
        return Err(surrealdb::Error::Db(surrealdb::error::Db::Io(
            io::Error::new(io::ErrorKind::NotFound, "WORKER_DOES_NOT_EXIST"),
        )));
    }

    Ok(())
}

/// Perform the initial heartbeat and registration of the worker
pub async fn initial_heartbeat(
    conn: Surreal<Any>,
    config: Config,
    worker: WorkerContext,
    storage_type: &str,
) -> Result<(), surrealdb::Error> {
    reenqueue_orphaned(conn.clone(), &config).await?;
    register_worker(conn, config, worker, storage_type).await?;
    Ok(())
}

/// Create a stream that sends keep-alive signals at regular intervals
pub fn keep_alive_stream(
    conn: Surreal<Any>,
    config: Config,
    worker: WorkerContext,
) -> impl Stream<Item = Result<(), surrealdb::Error>> + Send {
    stream::unfold((), move |_| {
        let register = keep_alive(conn.clone(), config.clone(), worker.clone());
        let interval = apalis_core::timer::Delay::new(*config.keep_alive());

        interval.then(move |_| register.map(|res| Some((res, ()))))
    })
}
