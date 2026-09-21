use super::*;
use drasi::v1::{
    source_service_server::{SourceService, SourceServiceServer},
    BootstrapRequest, BootstrapResponse, HealthCheckResponse, SourceChange, StreamEventResponse,
    SubmitEventRequest, SubmitEventResponse,
};
use std::pin::Pin;
use tonic::{Response, Status, Streaming};

type Responses<Item> = Pin<Box<dyn futures::Stream<Item = Result<Item, Status>> + Send>>;

struct AckServer {
    replies: Vec<Result<StreamEventResponse, Status>>,
}

#[tonic::async_trait]
impl SourceService for AckServer {
    type StreamEventsStream = Responses<StreamEventResponse>;
    type RequestBootstrapStream = Responses<BootstrapResponse>;

    async fn stream_events(
        &self,
        request: Request<Streaming<SourceChange>>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        let mut incoming = request.into_inner();
        let mut count = 0;
        while incoming.message().await?.is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
        Ok(Response::new(Box::pin(tokio_stream::iter(
            self.replies.clone(),
        ))))
    }

    async fn submit_event(
        &self,
        _: Request<SubmitEventRequest>,
    ) -> Result<Response<SubmitEventResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }

    async fn request_bootstrap(
        &self,
        _: Request<BootstrapRequest>,
    ) -> Result<Response<Self::RequestBootstrapStream>, Status> {
        Err(Status::unimplemented("unused"))
    }

    async fn health_check(&self, _: Request<()>) -> Result<Response<HealthCheckResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
}

fn reply(success: bool, processed: u64) -> Result<StreamEventResponse, Status> {
    Ok(StreamEventResponse {
        success,
        events_processed: processed,
        ..Default::default()
    })
}

#[tokio::test]
async fn adaptive_send_and_close_require_complete_cumulative_acks() {
    let cases = [
        (
            "repeated final cumulative total",
            vec![reply(true, 1), reply(true, 3), reply(true, 3)],
            true,
        ),
        ("single final total", vec![reply(true, 3)], true),
        ("empty stream", vec![], false),
        (
            "short cumulative total whose sum equals sent",
            vec![reply(true, 1), reply(true, 2)],
            false,
        ),
        ("failure without error text", vec![reply(false, 3)], false),
        (
            "failure after final total",
            vec![reply(true, 3), reply(false, 3)],
            false,
        ),
        (
            "decreasing total",
            vec![reply(true, 2), reply(true, 1), reply(true, 3)],
            false,
        ),
        ("excessive total", vec![reply(true, 4)], false),
        (
            "transport error after final total",
            vec![reply(true, 3), Err(Status::internal("stream failed"))],
            false,
        ),
    ];
    for (label, replies, expected_success) in cases {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let incoming = futures::stream::unfold(listener, |listener| async {
            let connection = listener.accept().await.map(|(stream, _)| stream);
            Some((connection, listener))
        });
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(SourceServiceServer::new(AckServer { replies }))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let events = (1..=3).map(|ordinal| {
            serde_json::from_value(serde_json::json!({
                "op":"i", "reactivatorStart_ns":1, "reactivatorEnd_ns":1,
                "payload":{"source":{"db":"test","table":"node","ts_ns":1,"lsn":ordinal},
                    "before":null,"after":{"id":format!("item-{ordinal}"),"labels":["Item"],"properties":{"ordinal":ordinal}}}
            })).unwrap()
        }).collect();
        let client = Arc::new(Mutex::new(None));
        let sender_client = client.clone();
        let send = tokio::spawn(async move {
            AdaptiveGrpcSourceChangeDispatcher::send_batch(
                sender_client,
                events,
                "test-source".to_owned(),
                endpoint,
                5,
            )
            .await
        });
        let mut dispatcher = AdaptiveGrpcSourceChangeDispatcher {
            host: "127.0.0.1".to_owned(),
            port: 0,
            source_id: "test-source".to_owned(),
            tls: false,
            timeout_seconds: 5,
            adaptive_config: AdaptiveBatchConfig::default(),
            event_tx: None,
            batcher_handle: Some(send),
            client: client.clone(),
            failure: None,
        };
        let result = dispatcher.close().await;
        server.abort();
        let _ = server.await;
        assert_eq!(result.is_ok(), expected_success, "{label}: {result:?}");
        assert!(client.lock().await.is_none(), "{label}");
        assert_eq!(dispatcher.failure.is_some(), !expected_success, "{label}");
    }
}
