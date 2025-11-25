use apalis_core::backend::{BackendExt, Vacuum};
use ulid::Ulid;

use crate::{CompactType, SurrealStorage, query_file_as_str};

impl<Args, F, Decode> Vacuum for SurrealStorage<Args, Decode, F>
where
    Self:
        BackendExt<IdType = Ulid, Codec = Decode, Error = surrealdb::Error, Compact = CompactType>,
    F: Send,
    Decode: Send,
    Args: Send,
{
    async fn vacuum(&mut self) -> Result<usize, Self::Error> {
        let query = query_file_as_str("queries/backend/vacuum.surql").map_err(|e| *e)?;

        let mut res = self.conn.query(query).await?;

        let res: Option<usize> = res.take(1)?;

        Ok(res.unwrap_or(0))
    }
}
