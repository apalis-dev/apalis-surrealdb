use std::{collections::HashSet, str::FromStr};

use apalis_core::{
    backend::{BackendExt, TaskResult, WaitForCompletion},
    task::{status::Status, task_id::TaskId},
};
use futures::{StreamExt, stream::BoxStream};
use serde::{Deserialize, de::DeserializeOwned};
use ulid::Ulid;

use crate::{CompactType, SurrealStorage, query_file_as_str};

#[derive(Debug, Deserialize)]
struct ResultRow {
    pub id: Option<String>,
    pub status: Option<String>,
    pub result: Option<String>,
}

impl<O: Send + 'static, Args, Decode, F> WaitForCompletion<O> for SurrealStorage<Args, Decode, F>
where
    Self:
        BackendExt<IdType = Ulid, Codec = Decode, Error = surrealdb::Error, Compact = CompactType>,
    Result<O, String>: DeserializeOwned,
{
    type ResultStream = BoxStream<'static, Result<TaskResult<O>, Self::Error>>;

    fn wait_for(
        &self,
        task_ids: impl IntoIterator<Item = apalis_core::task::task_id::TaskId<Self::IdType>>,
    ) -> Self::ResultStream {
        let conn = self.conn.clone();
        let ids: HashSet<String> = task_ids.into_iter().map(|id| id.to_string()).collect();

        let stream = futures::stream::unfold(ids, move |mut remaining_ids| {
            let conn = conn.clone();

            async move {
                if remaining_ids.is_empty() {
                    return None;
                }

                let query =
                    query_file_as_str("queries/backend/fetch_completed_tasks.surql").ok()?;

                let ids_vec: Vec<String> = remaining_ids.iter().cloned().collect();
                let ids_vec = serde_json::to_string(&ids_vec).unwrap();

                let mut res = conn.query(query).bind(("job_ids", ids_vec)).await.ok()?;

                let rows: Option<Vec<ResultRow>> = res.take(0).ok()?;

                let rows = rows.unwrap_or_default();

                if rows.is_empty() {
                    apalis_core::timer::sleep(std::time::Duration::from_millis(500)).await;
                    return Some((futures::stream::iter(vec![]), remaining_ids));
                }

                let mut results = Vec::new();
                for row in rows {
                    let task_id = row.id.clone().unwrap();
                    remaining_ids.remove(&task_id);

                    // Here we would normally decode the output O from the row
                    // For simplicity, we assume O is String and the output is stored in row.output
                    let result: Result<O, String> =
                        serde_json::from_str(&row.result.unwrap()).unwrap();

                    results.push(Ok(TaskResult::new(
                        TaskId::from_str(&task_id).ok()?,
                        Status::from_str(&row.status.unwrap()).ok()?,
                        result,
                    )));
                }

                Some((futures::stream::iter(results), remaining_ids))
            }
        });

        stream.flatten().boxed()
    }

    fn check_status(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>> + Send,
    ) -> impl Future<Output = Result<Vec<TaskResult<O>>, Self::Error>> + Send {
        let conn = self.conn.clone();
        let ids: Vec<String> = task_ids.into_iter().map(|id| id.to_string()).collect();

        async move {
            let query =
                query_file_as_str("queries/backend/fetch_completed_tasks.surql").map_err(|e| *e)?;
            let ids = serde_json::to_string(&ids).unwrap();
            let mut res = conn.query(query).bind(("ids", ids)).await?;

            let rows: Option<Vec<ResultRow>> = res.take(0)?;
            let rows = rows.unwrap_or_default();

            let mut results = Vec::new();

            for row in rows {
                let task_id = TaskId::from_str(&row.id.unwrap()).map_err(|_| {
                    surrealdb::Error::Db(surrealdb::error::Db::Thrown("Invalid task ID".into()))
                })?;

                let result: Result<O, String> = serde_json::from_str(&row.result.unwrap())
                    .map_err(|_| {
                        surrealdb::Error::Db(surrealdb::error::Db::Thrown(
                            "Failed to decode result".into(),
                        ))
                    })?;

                results.push(TaskResult::new(
                    task_id,
                    row.status.unwrap().parse().map_err(|_| {
                        surrealdb::Error::Db(surrealdb::error::Db::Thrown(
                            "Invalid status value".into(),
                        ))
                    })?,
                    result,
                ));
            }

            Ok(results)
        }
    }
}
