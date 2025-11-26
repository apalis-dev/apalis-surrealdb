use apalis_core::backend::{BackendExt, FetchById, codec::Codec};
use apalis_core::task::{Task, task_id::TaskId};
use apalis_sql::from_row::{FromRowError, TaskRow};
use ulid::Ulid;

use crate::from_record::{RawSurrealTask, SurrealTaskRecord};
use crate::{CompactType, SurrealContext, SurrealStorage, query_file_as_str};

impl<Args, D, F> FetchById<Args> for SurrealStorage<Args, D, F>
where
    Self: BackendExt<
            Context = SurrealContext,
            Compact = CompactType,
            IdType = Ulid,
            Error = surrealdb::Error,
        >,
    D: Codec<Args, Compact = CompactType>,
    D::Error: std::error::Error + Send + Sync + 'static,
    Args: 'static,
{
    fn fetch_by_id(
        &mut self,
        task_id: &TaskId<Self::IdType>,
    ) -> impl Future<Output = Result<Option<Task<Args, Self::Context, Self::IdType>>, Self::Error>> + Send
    {
        let conn = self.conn.clone();
        let task_id = task_id.to_string();

        async move {
            let query = query_file_as_str("queries/backend/find_by_id.surql").map_err(|e| *e)?;
            let mut res = conn.query(query).bind(("task_id", task_id)).await?;

            let res: Option<RawSurrealTask> = res.take(0)?;

            let task = res
                .map(|r| {
                    let r: SurrealTaskRecord = r.into();
                    let row: TaskRow = r
                        .try_into()
                        .map_err(|e: surrealdb::Error| FromRowError::DecodeError(e.into()))?;

                    row.try_into_task_compact().and_then(|t| {
                        t.try_map(|a| {
                            D::decode(&a).map_err(|e| FromRowError::DecodeError(e.into()))
                        })
                    })
                })
                .transpose()
                .map_err(|e| surrealdb::error::Db::Thrown(e.to_string()))?;

            Ok(task)
        }
    }
}
