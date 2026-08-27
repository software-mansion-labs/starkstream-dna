use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use apibara_dna_protocol::dna::stream::{stream_data_response, StreamDataResponse};
use futures::Stream;
use tokio::{sync::mpsc, time::Interval};
use tokio_util::sync::CancellationToken;

use crate::data_stream::ActiveStreamGuard;

pub struct ResponseStreamWithHeartbeat {
    rx: mpsc::Receiver<Result<StreamDataResponse, tonic::Status>>,
    interval: Interval,
    _active: ActiveStreamGuard,
    ct: CancellationToken,
}

impl ResponseStreamWithHeartbeat {
    pub(crate) fn new(
        rx: mpsc::Receiver<Result<StreamDataResponse, tonic::Status>>,
        heartbeat_interval: Duration,
        active: ActiveStreamGuard,
        ct: CancellationToken,
    ) -> Self {
        let mut interval = tokio::time::interval(heartbeat_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.reset();

        Self {
            rx,
            interval,
            _active: active,
            ct,
        }
    }
}

impl Drop for ResponseStreamWithHeartbeat {
    fn drop(&mut self) {
        self.ct.cancel();
    }
}

impl Stream for ResponseStreamWithHeartbeat {
    type Item = Result<StreamDataResponse, tonic::Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(data) = self.rx.poll_recv(cx) {
            self.interval.reset();
            return Poll::Ready(data);
        }

        if self.interval.poll_tick(cx).is_ready() {
            let message = StreamDataResponse {
                message: Some(stream_data_response::Message::Heartbeat(Default::default())),
            };

            return Poll::Ready(Some(Ok(message)));
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use crate::data_stream::ActiveStreamGuard;

    use super::ResponseStreamWithHeartbeat;

    #[tokio::test]
    async fn dropping_response_stream_cancels_worker() {
        let (_tx, rx) = mpsc::channel(1);
        let ct = CancellationToken::new();
        let active = apibara_observability::meter("response_stream_test")
            .i64_up_down_counter("active")
            .build();
        let stream = ResponseStreamWithHeartbeat::new(
            rx,
            Duration::from_secs(30),
            ActiveStreamGuard::new(active),
            ct.clone(),
        );

        assert!(!ct.is_cancelled());
        drop(stream);
        assert!(ct.is_cancelled());
    }
}
