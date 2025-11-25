use apalis_core::backend::{BackendExt, ListQueues, QueueInfo};
use ulid::Ulid;

use crate::{CompactType, SurrealContext, SurrealStorage, query_file_as_str};

impl<Args, D, F> ListQueues for SurrealStorage<Args, D, F>
where
    Self: BackendExt<
            Context = SurrealContext,
            Compact = CompactType,
            IdType = Ulid,
            Error = surrealdb::Error,
        >,
{
    fn list_queues(&self) -> impl Future<Output = Result<Vec<QueueInfo>, Self::Error>> + Send {
        let conn = self.conn.clone();

        async move {
            let query = query_file_as_str("queries/backend/list_queues.surql").map_err(|e| *e)?;
            let mut res = conn.query(query).await?;
            let res: Option<Vec<QueueInfo>> = res.take(2)?;

            match res {
                Some(val) => Ok(val),
                None => Ok(vec![]),
            }
        }
    }
}
