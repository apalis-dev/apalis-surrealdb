use std::io;

use apalis_core::{
    error::{AbortError, BoxDynError},
    layers::{Layer, Service},
    task::{Parts, status::Status},
    worker::{context::WorkerContext, ext::ack::Acknowledge},
};
use futures::{FutureExt, future::BoxFuture};
use serde::Serialize;
use surrealdb::{RecordId, Surreal, engine::any::Any};
use ulid::Ulid;

use crate::{SurrealContext, SurrealTask, query_file_as_str};

#[derive(Clone, Debug)]
pub struct SurrealAck {
    conn: Surreal<Any>,
}

impl SurrealAck {
    pub fn new(db: Surreal<Any>) -> Self {
        Self { conn: db }
    }
}

impl<Res: Serialize + 'static> Acknowledge<Res, SurrealContext, Ulid> for SurrealAck {
    type Error = surrealdb::Error;
    type Future = BoxFuture<'static, Result<(), Self::Error>>;
    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        ctx: &Parts<SurrealContext, Ulid>,
    ) -> Self::Future {
        let task_id = ctx.task_id;
        let worker_id = ctx.ctx.lock_by().clone();

        // Workflows need special handling to serialize the response correctly
        let response = serde_json::to_string(&res.as_ref().map_err(|e| e.to_string()));

        let status = calculate_status(ctx, res);
        ctx.status.store(status.clone());

        let attempts = ctx.attempt.current() as i32;
        let conn = self.conn.clone();
        let res = response.map_err(|e| {
            surrealdb::Error::Db(surrealdb::error::Db::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                e.to_string(),
            )))
        });
        let status = status.to_string();

        async move {
            let task_id = task_id
                .ok_or(surrealdb::error::Db::FdNotFound {
                    name: "TASK_ID_FOR_ACk".into(),
                })?
                .to_string();
            let worker_id = worker_id.ok_or(surrealdb::error::Db::FdNotFound {
                name: "WORKER_ID_LOCK".into(),
            })?;

            let last_result = res?;
            let query = query_file_as_str("queries/task/ack.surql").map_err(|e| *e)?;

            let mut result = conn
                .query(query)
                .bind(("task_id", RecordId::from_table_key("jobs", task_id.clone())))
                .bind(("attempts", attempts))
                .bind(("res_ok", last_result))
                .bind(("status", status))
                .bind((
                    "worker_id",
                    RecordId::from_table_key("workers", worker_id.clone()),
                ))
                .await?;
            let result: Option<i32> = result.take(1)?;

            if result.is_none() || result.map(|r| r == 0).unwrap_or(false) {
                return Err(surrealdb::Error::Db(surrealdb::error::Db::IdNotFound {
                    rid: format!("task_id: {}, worker_id: {}", task_id, worker_id),
                }));
            }

            Ok(())
        }
        .boxed()
    }
}

pub(crate) fn calculate_status<Res>(
    parts: &Parts<SurrealContext, Ulid>,
    res: &Result<Res, BoxDynError>,
) -> Status {
    match &res {
        Ok(_) => Status::Done,
        Err(e) => match e {
            _ if parts.ctx.max_attempts() as usize <= parts.attempt.current() => Status::Killed,
            e if e.downcast_ref::<AbortError>().is_some() => Status::Killed,
            _ => Status::Failed,
        },
    }
}

pub(crate) async fn lock_task(
    conn: &Surreal<Any>,
    task_id: &Ulid,
    worker_id: &str,
) -> Result<(), surrealdb::Error> {
    let task_id = task_id.to_string();
    let worker_id = worker_id.to_string();
    let query = query_file_as_str("queries/task/lock.surql").map_err(|e| *e)?;

    let mut res = conn
        .query(query)
        .bind(("task_id", RecordId::from_table_key("jobs", task_id.clone())))
        .bind((
            "worker_id",
            RecordId::from_table_key("workers", worker_id.clone()),
        ))
        .await?;

    let res: Option<i32> = res.take(1)?;

    if res.is_none() || res.map(|r| r == 0).unwrap_or(false) {
        return Err(surrealdb::Error::Db(surrealdb::error::Db::IdNotFound {
            rid: format!("task_id: {}, worker_id: {}", task_id, worker_id),
        }));
    }

    Ok(())
}

#[derive(Clone, Debug)]
pub struct LockTaskLayer {
    conn: Surreal<Any>,
}

impl LockTaskLayer {
    pub fn new(conn: Surreal<Any>) -> Self {
        Self { conn }
    }
}

impl<S> Layer<S> for LockTaskLayer {
    type Service = LockTaskService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        LockTaskService {
            inner,
            conn: self.conn.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LockTaskService<S> {
    inner: S,
    conn: Surreal<Any>,
}

impl<S, Args> Service<SurrealTask<Args>> for LockTaskService<S>
where
    S: Service<SurrealTask<Args>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    Args: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, mut req: SurrealTask<Args>) -> Self::Future {
        let conn = self.conn.clone();
        let worker_id = req
            .parts
            .data
            .get::<WorkerContext>()
            .map(|w| w.name().to_owned())
            .unwrap();

        let parts = &req.parts;
        let task_id = match &parts.task_id {
            Some(id) => *id.inner(),
            None => {
                return async {
                    Err(surrealdb::Error::Db(surrealdb::error::Db::FdNotFound {
                        name: "TASK_ID_FOR_LOCK".into(),
                    })
                    .into())
                }
                .boxed();
            }
        };

        req.parts.ctx = req.parts.ctx.with_lock_by(Some(worker_id.clone()));
        let fut = self.inner.call(req);
        async move {
            lock_task(&conn, &task_id, &worker_id).await?;

            fut.await.map_err(|e| e.into())
        }
        .boxed()
    }
}
