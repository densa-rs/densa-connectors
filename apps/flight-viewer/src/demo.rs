//! A local Flight service for `--demo`: any query's answer is a million web requests (a
//! service log) in four endpoints, made as they are sent.

use std::error::Error;
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;

use arrow_array::{
    ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use arrow_schema::ArrowError;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

const ROWS: i64 = 1_000_000;
const ENDPOINTS: i64 = 4;
const BATCH: i64 = 16_384;

/// Starts the service on a free local port, on a thread of its own; its URL.
pub fn serve() -> Result<String, Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    listener.set_nonblocking(true)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    thread::Builder::new()
        .name("demo flight service".to_owned())
        .spawn(move || {
            runtime.block_on(async move {
                let Ok(listener) = tokio::net::TcpListener::from_std(listener) else {
                    return;
                };
                let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
                let served = Server::builder()
                    .add_service(FlightServiceServer::new(Requests))
                    .serve_with_incoming(incoming)
                    .await;
                if let Err(error) = served {
                    eprintln!("demo service stopped: {error}");
                }
            });
        })?;
    Ok(format!("grpc://{address}"))
}

/// Requests `start..end` as a batch.
fn requests(start: i64, end: i64) -> Result<RecordBatch, ArrowError> {
    const SERVICES: [&str; 6] = [
        "gateway", "auth", "catalog", "checkout", "search", "storage",
    ];
    const PATHS: [&str; 6] = ["/", "/login", "/items", "/cart", "/search", "/files"];
    const METHODS: [&str; 4] = ["GET", "GET", "POST", "PUT"];
    let ids: Vec<i64> = (start..end).collect();
    // Lossless: small indices.
    let pick = |i: i64, n: i64| (i * 7_919 % n) as usize;
    let status = |i: i64| match i % 97 {
        0 => 500,
        1..=3 => 404,
        _ => 200,
    };
    // Whole hundredths, so values print short.
    let millis = |i: i64| (200 + (i * 7_919) % 16_000) as f64 / 100.0;
    RecordBatch::try_from_iter([
        (
            "request",
            Arc::new(Int64Array::from(ids.clone())) as ArrayRef,
        ),
        (
            "time",
            Arc::new(
                TimestampMillisecondArray::from_iter_values(
                    ids.iter().map(|&i| 1_792_368_000_000 + i * 37),
                )
                .with_timezone("UTC"),
            ),
        ),
        (
            "service",
            Arc::new(StringArray::from_iter_values(
                ids.iter().map(|&i| SERVICES[pick(i, 6)]),
            )),
        ),
        (
            "method",
            Arc::new(StringArray::from_iter_values(
                ids.iter().map(|&i| METHODS[pick(i, 4)]),
            )),
        ),
        (
            "path",
            Arc::new(StringArray::from_iter_values(
                ids.iter().map(|&i| PATHS[pick(i + 3, 6)]),
            )),
        ),
        (
            "status",
            Arc::new(Int64Array::from_iter_values(ids.iter().map(|&i| status(i)))),
        ),
        (
            "ms",
            Arc::new(Float64Array::from_iter_values(
                ids.iter().map(|&i| millis(i)),
            )),
        ),
        (
            "bytes",
            Arc::new(Int64Array::from_iter_values(
                ids.iter().map(|&i| 200 + i * 31 % 40_000),
            )),
        ),
    ])
}

/// Answers every query with the requests, a quarter per endpoint (the ticket is its index).
struct Requests;

#[tonic::async_trait]
impl FlightService for Requests {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn get_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let endpoints = (0..ENDPOINTS)
            .map(|part| FlightEndpoint::new().with_ticket(Ticket::new(part.to_string())))
            .collect();
        Ok(Response::new(FlightInfo::new().with_endpoints(endpoints)))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner().ticket;
        let part: i64 = std::str::from_utf8(&ticket)
            .ok()
            .and_then(|text| text.parse().ok())
            .filter(|part| (0..ENDPOINTS).contains(part))
            .ok_or_else(|| Status::not_found("no such part"))?;
        let (start, end) = (part * ROWS / ENDPOINTS, (part + 1) * ROWS / ENDPOINTS);
        let batches = futures::stream::iter((start..end).step_by(BATCH as usize))
            .map(move |first| requests(first, (first + BATCH).min(end)).map_err(Into::into));
        let data = FlightDataEncoderBuilder::new()
            .build(batches)
            .map_err(Status::from)
            .boxed();
        Ok(Response::new(data))
    }

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_put(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn do_action(
        &self,
        _: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }

    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
}
