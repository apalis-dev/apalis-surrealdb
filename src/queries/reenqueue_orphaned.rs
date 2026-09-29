use std::time::Duration;

use futures::{FutureExt, Stream, stream};
use surrealdb::{Surreal, engine::any::Any};

use crate::{Config, query_file_as_str};

/// Re-enqueue tasks that were being processed by workers that have not sent a keep-alive signal within the specified duration
pub fn reenqueue_orphaned(
    conn: Surreal<Any>,
    config: &Config,
) -> impl Future<Output = Result<u64, surrealdb::Error>> + Send {
    let dead_for = config.reenqueue_orphaned_after().as_secs() as i64;
    let queue = config.queue().to_string();

    async move {
        let query =
            query_file_as_str("queries/backend/reenqueue_orphaned.surql").map_err(|e| *e)?;
        let mut res = conn
            .query(query)
            .bind(("dead_for", dead_for))
            .bind(("worker_type", queue))
            .await?;

        let tasks = res.take(1);

        match tasks {
            Ok(ts) => {
                let ts: Option<u64> = ts;

                if let Some(ts) = ts {
                    if ts > 0 {
                        log::info!(
                            "Re-enqueued {} orphaned tasks that were being processed by dead workers",
                            ts
                        );
                    }
                    return Ok(ts);
                }

                Ok(0)
            }

            Err(e) => {
                log::error!("Failed to re-enqueue orphaned tasks: {e}");
                Err(e)
            }
        }
    }
}

/// Create a stream that periodically re-enqueues orphaned tasks
pub fn reenqueue_orphaned_stream(
    conn: Surreal<Any>,
    config: Config,
    interval: Duration,
) -> impl Stream<Item = Result<u64, surrealdb::Error>> + Send {
    let config = config;
    stream::unfold((), move |_| {
        let pool = conn.clone();
        let config = config.clone();
        let interval = apalis_core::timer::Delay::new(interval);
        let fut = async move {
            interval.await;
            reenqueue_orphaned(pool, &config).await
        };

        fut.map(|res| Some((res, ())))
    })
}
