use apalis_core::backend::{BackendExt, Metrics, Statistic};
use serde::Deserialize;
use ulid::Ulid;

use crate::{CompactType, SurrealContext, SurrealStorage, query_file_as_str};

#[derive(Debug, Deserialize)]
struct StatisticRow {
    /// The priority of the statistic (lower number means higher priority)
    pub priority: i64,
    /// The statistics type
    pub r#type: String,
    /// Overall statistics of the backend
    pub statistic: String,
    /// The value of the statistic
    pub value: Option<f64>,
}

impl<Args, D, F> Metrics for SurrealStorage<Args, D, F>
where
    Self: BackendExt<
            Context = SurrealContext,
            Compact = CompactType,
            IdType = Ulid,
            Error = surrealdb::Error,
        >,
{
    fn global(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let conn = self.conn.clone();

        async move {
            let query = query_file_as_str("queries/backend/overview.surql").map_err(|e| *e)?;

            let mut res = conn.query(query).await?;
            let stats: Option<Vec<StatisticRow>> = res.take(2)?;

            let stats = stats
                .map(|ss| {
                    ss.into_iter()
                        .map(|s| Statistic {
                            title: s.statistic,
                            stat_type: super::stat_type_from_string(&s.r#type),
                            value: s.value.unwrap_or_default().to_string(),
                            priority: Some(s.priority as u64),
                        })
                        .collect()
                })
                .unwrap_or_default();

            Ok(stats)
        }
    }

    fn fetch_by_queue(
        &self,
        queue: &str,
    ) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let conn = self.conn.clone();
        let queue = queue.to_owned();

        async move {
            let query =
                query_file_as_str("queries/backend/overview_by_queue.surql").map_err(|e| *e)?;
            let mut res = conn.query(query).bind(("queue", queue)).await?;
            let stats: Option<Vec<StatisticRow>> = res.take(2)?;

            let stats = stats
                .map(|ss| {
                    ss.into_iter()
                        .map(|s| Statistic {
                            title: s.statistic,
                            stat_type: super::stat_type_from_string(&s.r#type),
                            value: s.value.unwrap_or_default().to_string(),
                            priority: Some(s.priority as u64),
                        })
                        .collect()
                })
                .unwrap_or_default();

            Ok(stats)
        }
    }
}
