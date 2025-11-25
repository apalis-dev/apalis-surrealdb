use apalis_core::backend::{queue::Queue, BackendExt, ConfigExt};
use ulid::Ulid;

use crate::{CompactType, SurrealContext, SurrealStorage};

impl<Args: Sync, D, F> ConfigExt for SurrealStorage<Args, D, F>
where
    Self: BackendExt<
        Context = SurrealContext,
        Compact = CompactType,
        IdType = Ulid,
        Error = surrealdb::Error,
    >,
{
    fn get_queue(&self) -> Queue {
        self.config.queue().clone()
    }
}
