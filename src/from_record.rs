use apalis_sql::from_row::TaskRow;
use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::RecordId;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct RawSurrealTask {
    pub(crate) id: Option<RecordId>,
    #[serde(with = "serde_bytes")]
    pub(crate) job: Vec<u8>,
    pub(crate) job_type: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) attempts: Option<i32>,
    pub(crate) max_attempts: Option<i32>,
    pub(crate) run_at: Option<i64>,
    pub(crate) last_result: Option<String>,
    pub(crate) lock_at: Option<i64>,
    pub(crate) lock_by: Option<RecordId>,
    pub(crate) done_at: Option<i64>,
    pub(crate) priority: Option<i32>,
    pub(crate) metadata: Option<String>,
}

impl From<RawSurrealTask> for SurrealTaskRecord {
    fn from(value: RawSurrealTask) -> Self {
        Self {
            id: value.id.map(|id| id.key().to_string()),
            job: value.job,
            job_type: value.job_type,
            status: value.status,
            attempts: value.attempts,
            max_attempts: value.max_attempts,
            run_at: value.run_at,
            last_result: value.last_result,
            lock_at: value.lock_at,
            lock_by: value.lock_by.map(|l| l.to_string()),
            done_at: value.done_at,
            priority: value.priority,
            metadata: value.metadata,
        }
    }
}

/// Represents Job Record in surrealDb
#[derive(Debug, Deserialize, Serialize)]
/// Represents a record from the jobs table in the database
///
/// This struct contains all the fields necessary to represent a task/job
/// stored in the SurrealDb store, including its execution state, metadata,
/// and scheduling information
pub(crate) struct SurrealTaskRecord {
    pub(crate) id: Option<String>,
    #[serde(with = "serde_bytes")]
    pub(crate) job: Vec<u8>,
    pub(crate) job_type: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) attempts: Option<i32>,
    pub(crate) max_attempts: Option<i32>,
    pub(crate) run_at: Option<i64>,
    pub(crate) last_result: Option<String>,
    pub(crate) lock_at: Option<i64>,
    pub(crate) lock_by: Option<String>,
    pub(crate) done_at: Option<i64>,
    pub(crate) priority: Option<i32>,
    pub(crate) metadata: Option<String>,
}

impl TryFrom<SurrealTaskRecord> for TaskRow {
    type Error = surrealdb::Error;
    fn try_from(value: SurrealTaskRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            job: value.job.to_vec(),
            id: value.id.ok_or_else(|| {
                surrealdb::Error::Db(surrealdb::error::Db::FdNotFound { name: "id".into() })
            })?,
            job_type: value.job_type.ok_or_else(|| {
                surrealdb::Error::Db(surrealdb::error::Db::FdNotFound {
                    name: "job_type".into(),
                })
            })?,
            status: value.status.ok_or_else(|| {
                surrealdb::Error::Db(surrealdb::error::Db::FdNotFound {
                    name: "status".into(),
                })
            })?,
            attempts: value.attempts.ok_or_else(|| {
                surrealdb::Error::Db(surrealdb::error::Db::FdNotFound {
                    name: "attempts".into(),
                })
            })? as usize,
            max_attempts: value.max_attempts.map(|v| v as usize),
            run_at: value.run_at.map(|ts| {
                Utc.timestamp_opt(ts, 0)
                    .single()
                    .ok_or_else(|| {
                        surrealdb::Error::Db(surrealdb::error::Db::ConvertTo {
                            from: ts.into(),
                            into: "DateTime".into(),
                        })
                    })
                    .unwrap()
            }),
            last_result: value
                .last_result
                .map(|res| serde_json::from_str(&res).unwrap_or(serde_json::Value::Null)),
            lock_at: value.lock_at.map(|ts| {
                Utc.timestamp_opt(ts, 0)
                    .single()
                    .ok_or_else(|| {
                        surrealdb::Error::Db(surrealdb::error::Db::ConvertTo {
                            from: ts.into(),
                            into: "DateTime".into(),
                        })
                    })
                    .unwrap()
            }),
            lock_by: value.lock_by,
            done_at: value.done_at.map(|ts| {
                Utc.timestamp_opt(ts, 0)
                    .single()
                    .ok_or_else(|| {
                        surrealdb::Error::Db(surrealdb::error::Db::CoerceTo {
                            from: ts.into(),
                            into: "DateTime".into(),
                        })
                    })
                    .unwrap()
            }),
            priority: value.priority.map(|p| p as usize),
            metadata: value
                .metadata
                .map(|meta| serde_json::from_str(&meta).unwrap_or(serde_json::Value::Null)),
        })
    }
}
