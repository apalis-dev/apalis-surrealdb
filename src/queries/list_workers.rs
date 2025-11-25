use apalis_core::backend::{BackendExt, ListWorkers, RunningWorker};
use serde::Deserialize;
use surrealdb::RecordId;
use ulid::Ulid;

use crate::{CompactType, SurrealContext, SurrealStorage, query_file_as_str};

#[derive(Debug, Deserialize)]
struct Worker {
    id: RecordId,
    worker_type: String,
    storage_name: String,
    layers: Option<String>,
    last_seen: i64,
    started_at: Option<i64>,
}

impl<Args: Sync, D, F> ListWorkers for SurrealStorage<Args, D, F>
where
    Self: BackendExt<
            Context = SurrealContext,
            Compact = CompactType,
            IdType = Ulid,
            Error = surrealdb::Error,
        >,
{
    fn list_workers(
        &self,
        queue: &str,
    ) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let queue = queue.to_owned();
        let limit = 100;
        let offset = 0;
        let conn = self.conn.clone();

        async move {
            let query = query_file_as_str("queries/backend/list_workers.surql").map_err(|e| *e)?;
            let mut res = conn
                .query(query)
                .bind(("worker_type", queue))
                .bind(("limit", limit))
                .bind(("offset", offset))
                .await?;

            let res: Option<Vec<Worker>> = res.take(0)?;

            let workers = res
                .map(|ws| {
                    ws.into_iter()
                        .map(|w| RunningWorker {
                            id: w.id.to_string(),
                            backend: w.storage_name,
                            started_at: w.started_at.unwrap_or_default() as u64,
                            last_heartbeat: w.last_seen as u64,
                            layers: w.layers.unwrap_or_default(),
                            queue: w.worker_type,
                        })
                        .collect()
                })
                .unwrap_or_default();

            Ok(workers)
        }
    }

    fn list_all_workers(
        &self,
    ) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let conn = self.conn.clone();
        let limit = 100;
        let offset = 0;

        async move {
            let query =
                query_file_as_str("queries/backend/list_all_workers.surql").map_err(|e| *e)?;
            let mut res = conn
                .query(query)
                .bind(("limit", limit))
                .bind(("offset", offset))
                .await?;

            let res: Option<Vec<Worker>> = res.take(0)?;

            let workers = res
                .map(|ws| {
                    ws.into_iter()
                        .map(|w| RunningWorker {
                            id: w.id.to_string(),
                            backend: w.storage_name,
                            started_at: w.started_at.unwrap_or_default() as u64,
                            last_heartbeat: w.last_seen as u64,
                            layers: w.layers.unwrap_or_default(),
                            queue: w.worker_type,
                        })
                        .collect()
                })
                .unwrap_or_default();

            Ok(workers)
        }
    }
}
