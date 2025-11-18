use mq::JobProcessor;
use std::{marker::PhantomData, time::Duration};
use time::OffsetDateTime;

use apalis_core::{
    layers::Identity,
    poller::{stream::BackendStream, Poller},
    request::{Request, RequestStream},
    worker::WorkerId,
    Backend, Codec,
};
use futures::{channel::mpsc::channel, StreamExt};

pub struct SurrealStorage<T> {
    consumer: mq_surreal::SurrealJobProcessor,
    producer: mq_surreal::SurrealProducer,
    _t: PhantomData<T>,
}

pub struct SurrealContext {
    kind: String,
    created_at: Option<OffsetDateTime>,
    updated_at: Option<OffsetDateTime>,
    scheduled_at: Option<OffsetDateTime>,
    // error_reason: Option<Value>,
    attempts: u16,
    max_attempts: u16,
    lease_time: Duration,
    priority: u8,
    unique_key: Option<String>,
}



impl<T: Send + 'static + Sync, Res> Backend<Request<T, ()>, Res> for SurrealStorage<T> {
    type Stream = BackendStream<RequestStream<Request<T, ()>>>;

    type Layer = Identity;

    fn poll<Svc>(self, _worker: WorkerId) -> Poller<Self::Stream> {
        let (rx, tx) = channel(10);
        let stream = tx.boxed();
        let heartbeat = async {
            let next = self.consumer.poll_next_job(&["queue"]).await;
            if let Ok(Some(task)) = next {}
        };

        Poller::new(stream, heartbeat)
    }
}
