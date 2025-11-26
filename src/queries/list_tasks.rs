use apalis_core::{
    backend::{BackendExt, Filter, ListAllTasks, ListTasks, codec::Codec},
    task::{Task, status::Status},
};
use apalis_sql::from_row::{FromRowError, TaskRow};
use ulid::Ulid;

use crate::{
    CompactType, SurrealContext, SurrealStorage,
    from_record::{RawSurrealTask, SurrealTaskRecord},
    query_file_as_str,
};

impl<Args, D, F> ListTasks<Args> for SurrealStorage<Args, D, F>
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
    fn list_tasks(
        &self,
        queue: &str,
        filter: &Filter,
    ) -> impl Future<Output = Result<Vec<Task<Args, Self::Context, Self::IdType>>, Self::Error>> + Send
    {
        let queue = queue.to_owned();
        let conn = self.conn.clone();
        let limit = filter.limit() as i32;
        let offset = filter.offset() as i32;
        let status = filter
            .status
            .as_ref()
            .unwrap_or(&Status::Pending)
            .to_string();

        async move {
            let query = query_file_as_str("queries/backend/list_jobs.surql").map_err(|e| *e)?;
            let mut res = conn
                .query(query)
                .bind(("status", status))
                .bind(("job_type", queue))
                .bind(("limit", limit))
                .bind(("offset", offset))
                .await?;

            let tasks: Option<Vec<RawSurrealTask>> = res.take(0)?;

            let tasks = tasks
                .map(|ts| {
                    ts.into_iter()
                        .map(|t| {
                            let t: SurrealTaskRecord = t.into();
                            let row: TaskRow = t.try_into().map_err(|e: surrealdb::Error| {
                                FromRowError::DecodeError(e.into())
                            })?;

                            row.try_into_task_compact().and_then(|t| {
                                t.try_map(|a| {
                                    D::decode(&a).map_err(|e| FromRowError::DecodeError(e.into()))
                                })
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| {
                            surrealdb::Error::Db(surrealdb::error::Db::Thrown(e.to_string()))
                        })
                })
                .unwrap_or(Ok(vec![]))?;

            Ok(tasks)
        }
    }
}

impl<Args, D, F> ListAllTasks for SurrealStorage<Args, D, F>
where
    Self: BackendExt<
            Context = SurrealContext,
            Compact = CompactType,
            IdType = Ulid,
            Error = surrealdb::Error,
        >,
{
    fn list_all_tasks(
        &self,
        filter: &Filter,
    ) -> impl Future<
        Output = Result<Vec<Task<Self::Compact, Self::Context, Self::IdType>>, Self::Error>,
    > + Send {
        let status = filter
            .status
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or(Status::Pending.to_string());

        let conn = self.conn.clone();
        let limit = filter.limit() as i32;
        let offset = filter.offset() as i32;

        async move {
            let query = query_file_as_str("queries/backend/list_all_jobs.surql").map_err(|e| *e)?;
            let mut res = conn
                .query(query)
                .bind(("status", status))
                .bind(("limit", limit))
                .bind(("offset", offset))
                .await?;

            let tasks: Option<Vec<SurrealTaskRecord>> = res.take(0)?;

            let tasks = tasks
                .map(|ts| {
                    ts.into_iter()
                        .map(|t| {
                            let row: TaskRow = t.try_into()?;

                            row.try_into_task_compact().map_err(|e| {
                                surrealdb::Error::Db(surrealdb::error::Db::Thrown(e.to_string()))
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .unwrap_or(Ok(vec![]))?;

            Ok(tasks)
        }
    }
}
