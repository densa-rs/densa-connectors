//! Results from a local Flight service: tickets, paths and Flight SQL queries read endpoint
//! by endpoint, row numbers, failures, retries with growing waits, and a stream that stops
//! when dropped.

use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, UInt64Type};
use arrow_array::{ArrayRef, Float64Array, RecordBatch, StringArray};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use densa_flight::{FlightEvent, FlightOptions, FlightStream, RequestError};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use tonic::metadata::MetadataMap;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

const WAIT: Duration = Duration::from_secs(8);

type Data = BoxStream<'static, Result<FlightData, Status>>;
type DoGet = dyn Fn(&[u8], &MetadataMap) -> Result<Data, Status> + Send + Sync;
type GetInfo = dyn Fn(FlightDescriptor) -> Result<FlightInfo, Status> + Send + Sync;

/// A Flight service answering `DoGet` and `GetFlightInfo` with the closures given.
#[derive(Clone)]
struct Service {
    do_get: Arc<DoGet>,
    get_info: Arc<GetInfo>,
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("sensor", DataType::Utf8, false),
        Field::new("celsius", DataType::Float64, true),
    ]))
}

fn rows(range: std::ops::Range<i32>) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from_iter_values(
                range.clone().map(|i| format!("s{i}")),
            )) as ArrayRef,
            Arc::new(Float64Array::from_iter_values(range.map(f64::from))),
        ],
    )
    .unwrap()
}

/// `batches` as Flight data, the schema first (so an empty result still has one).
fn encoded(batches: Vec<RecordBatch>) -> Data {
    let batches = futures::stream::iter(batches.into_iter().map(Ok));
    FlightDataEncoderBuilder::new()
        .with_schema(schema())
        .build(batches)
        .map_err(Status::from)
        .boxed()
}

fn no_info() -> Arc<GetInfo> {
    Arc::new(|_| Err(Status::unimplemented("no flight info here")))
}

/// Serves `service` on a free local port; its URL.
fn serve(service: Service) -> String {
    serve_counted(service).0
}

/// Serves `service` on a free local port; its URL, and how many connections it accepted.
fn serve_counted(service: Service) -> (String, Arc<AtomicUsize>) {
    let connections = Arc::new(AtomicUsize::new(0));
    let accepted = connections.clone();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            let incoming =
                tokio_stream::wrappers::TcpListenerStream::new(listener).inspect(move |_| {
                    accepted.fetch_add(1, Ordering::SeqCst);
                });
            Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
    });
    (format!("grpc://{address}"), connections)
}

/// Serves the batches each ticket names: `t3` is rows 0..3.
fn by_ticket() -> String {
    serve(Service {
        do_get: Arc::new(|ticket, _| {
            let text = std::str::from_utf8(ticket).unwrap_or_default();
            let count: i32 = text
                .trim_start_matches('t')
                .parse()
                .map_err(|_| Status::not_found(text))?;
            Ok(encoded(vec![rows(0..count)]))
        }),
        get_info: no_info(),
    })
}

/// What a stream handed over, as text, to its end (or `WAIT`).
fn events(stream: &mut FlightStream) -> Vec<String> {
    let mut seen = Vec::new();
    let until = Instant::now() + WAIT;
    while Instant::now() < until {
        let Some(event) = stream.wait_next(WAIT) else {
            break;
        };
        let (text, last) = describe(event);
        seen.push(text);
        if last {
            break;
        }
    }
    seen
}

fn describe(event: FlightEvent) -> (String, bool) {
    match event {
        FlightEvent::Schema(schema) => {
            let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            (format!("schema {}", names.join(",")), false)
        }
        FlightEvent::Batch(batch) => (format!("batch {:?}", celsius(&batch)), false),
        FlightEvent::Finished => ("finished".to_owned(), true),
        FlightEvent::Retrying { retry_in, .. } => {
            (format!("retrying {}", retry_in.as_millis()), false)
        }
        FlightEvent::Failed { reason } => (format!("failed {reason}"), true),
        other => (format!("{other:?}"), false),
    }
}

fn celsius(batch: &RecordBatch) -> Vec<f64> {
    batch
        .column(1)
        .as_primitive::<Float64Type>()
        .values()
        .to_vec()
}

#[test]
fn a_ticket_hands_over_the_schema_then_its_rows_then_the_end() {
    let mut stream = FlightOptions::ticket(by_ticket(), "t3")
        .connect(|| {})
        .unwrap();
    assert_eq!(
        events(&mut stream),
        ["schema sensor,celsius", "batch [0.0, 1.0, 2.0]", "finished"]
    );
    assert_eq!(stream.wait_next(WAIT).map(|e| describe(e).0), None);
    assert!(stream.is_finished());
}

#[test]
fn a_path_is_read_endpoint_by_endpoint_in_order() {
    let asked = Arc::new(Mutex::new(Vec::new()));
    let seen = asked.clone();
    let url = serve(Service {
        do_get: Arc::new(|ticket, _| {
            let first = if ticket == b"first" { 0 } else { 10 };
            Ok(encoded(vec![rows(first..first + 2)]))
        }),
        get_info: Arc::new(move |descriptor| {
            seen.lock().unwrap().push(descriptor.path.join("/"));
            Ok(FlightInfo::new()
                .try_with_schema(&schema())
                .unwrap()
                .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new("first")))
                .with_endpoint(
                    FlightEndpoint::new()
                        .with_ticket(Ticket::new("second"))
                        .with_location("arrow-flight-reuse-connection://?"),
                ))
        }),
    });
    let mut stream = FlightOptions::path(url, ["plant", "3"])
        .connect(|| {})
        .unwrap();
    assert_eq!(
        events(&mut stream),
        [
            "schema sensor,celsius",
            "batch [0.0, 1.0]",
            "batch [10.0, 11.0]",
            "finished"
        ]
    );
    assert_eq!(*asked.lock().unwrap(), ["plant/3"]);
}

#[test]
fn an_endpoint_elsewhere_is_read_from_its_location() {
    let other = by_ticket();
    let url = serve(Service {
        do_get: Arc::new(|_, _| Err(Status::not_found("not here"))),
        get_info: Arc::new(move |_| {
            Ok(FlightInfo::new().with_endpoint(
                FlightEndpoint::new()
                    .with_ticket(Ticket::new("t2"))
                    .with_location("unknown://nowhere")
                    .with_location(other.clone()),
            ))
        }),
    });
    let mut stream = FlightOptions::command(url, "anything")
        .connect(|| {})
        .unwrap();
    assert_eq!(
        events(&mut stream),
        ["schema sensor,celsius", "batch [0.0, 1.0]", "finished"]
    );
}

#[cfg(feature = "sql")]
#[test]
fn a_flight_sql_query_is_sent_as_a_statement_command() {
    use arrow_flight::sql::{Any, CommandStatementQuery};
    use prost::Message;
    let url = serve(Service {
        do_get: Arc::new(|ticket, _| {
            assert_eq!(ticket, b"SELECT * FROM readings");
            Ok(encoded(vec![rows(0..1)]))
        }),
        get_info: Arc::new(|descriptor| {
            let any = Any::decode(&*descriptor.cmd)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            let command: CommandStatementQuery = any
                .unpack()
                .map_err(|e| Status::invalid_argument(e.to_string()))?
                .ok_or_else(|| Status::invalid_argument("not a statement"))?;
            let ticket = Ticket::new(command.query);
            Ok(FlightInfo::new().with_endpoint(FlightEndpoint::new().with_ticket(ticket)))
        }),
    });
    let mut stream = FlightOptions::sql(url, "SELECT * FROM readings")
        .connect(|| {})
        .unwrap();
    assert_eq!(
        events(&mut stream),
        ["schema sensor,celsius", "batch [0.0]", "finished"]
    );
}

#[cfg(feature = "sql")]
#[test]
fn a_flight_sql_service_is_asked_for_its_tables() {
    use arrow_flight::sql::{Any, CommandGetTables};
    use prost::Message;
    let url = serve(Service {
        do_get: Arc::new(|ticket, _| {
            assert_eq!(ticket, b"tables");
            Ok(encoded(vec![rows(0..2)]))
        }),
        get_info: Arc::new(|descriptor| {
            let any = Any::decode(&*descriptor.cmd)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            let command: CommandGetTables = any
                .unpack()
                .map_err(|e| Status::invalid_argument(e.to_string()))?
                .ok_or_else(|| Status::invalid_argument("not GetTables"))?;
            assert!(!command.include_schema, "names only");
            assert_eq!(command.catalog, None, "every catalog");
            let ticket = Ticket::new("tables");
            Ok(FlightInfo::new().with_endpoint(FlightEndpoint::new().with_ticket(ticket)))
        }),
    });
    let mut stream = FlightOptions::sql_tables(url).connect(|| {}).unwrap();
    assert_eq!(
        events(&mut stream),
        ["schema sensor,celsius", "batch [0.0, 1.0]", "finished"]
    );
}

#[test]
fn an_empty_result_still_has_its_schema() {
    let url = serve(Service {
        do_get: Arc::new(|_, _| Ok(encoded(Vec::new()))),
        get_info: Arc::new(|_| Ok(FlightInfo::new().try_with_schema(&schema()).unwrap())),
    });
    let mut stream = FlightOptions::ticket(url.clone(), "x")
        .connect(|| {})
        .unwrap();
    assert_eq!(
        events(&mut stream),
        ["schema sensor,celsius", "finished"],
        "from DoGet"
    );
    let mut stream = FlightOptions::path(url, ["none"]).connect(|| {}).unwrap();
    assert_eq!(
        events(&mut stream),
        ["schema sensor,celsius", "finished"],
        "from the flight info, with no endpoint"
    );
}

#[test]
fn row_numbers_count_on_across_endpoints() {
    let url = serve(Service {
        do_get: Arc::new(|ticket, _| match ticket {
            b"a" => Ok(encoded(vec![rows(0..2), rows(2..3)])),
            _ => Ok(encoded(vec![rows(3..5)])),
        }),
        get_info: Arc::new(|_| {
            Ok(FlightInfo::new()
                .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new("a")))
                .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new("b"))))
        }),
    });
    let mut stream = FlightOptions::path(url, ["x"])
        .row_numbers("row")
        .connect(|| {})
        .unwrap();
    let mut numbers = Vec::new();
    let mut schema = None;
    while let Some(event) = stream.wait_next(WAIT) {
        match event {
            FlightEvent::Schema(s) => schema = Some(s),
            FlightEvent::Batch(batch) => {
                assert_eq!(Some(&batch.schema()), schema.as_ref());
                numbers.extend(
                    batch
                        .column(2)
                        .as_primitive::<UInt64Type>()
                        .values()
                        .iter()
                        .copied(),
                );
            }
            FlightEvent::Finished => break,
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(numbers, [0, 1, 2, 3, 4]);
    let schema = schema.unwrap();
    assert_eq!(schema.field(2).name(), "row");
    assert!(!schema.field(2).is_nullable());
}

#[test]
fn a_row_number_column_that_is_taken_fails_without_retrying() {
    let mut stream = FlightOptions::ticket(by_ticket(), "t1")
        .row_numbers("sensor")
        .retry(Duration::from_millis(10), Duration::from_millis(10))
        .connect(|| {})
        .unwrap();
    let seen = events(&mut stream);
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert!(
        seen[0].starts_with("failed") && seen[0].contains("sensor"),
        "{seen:?}"
    );
}

#[test]
fn headers_go_with_every_call() {
    let tokens = Arc::new(Mutex::new(Vec::new()));
    let seen = tokens.clone();
    let url = serve(Service {
        do_get: Arc::new(move |_, metadata| {
            let token = metadata.get("authorization").and_then(|v| v.to_str().ok());
            seen.lock()
                .unwrap()
                .push(token.unwrap_or_default().to_owned());
            Ok(encoded(Vec::new()))
        }),
        get_info: no_info(),
    });
    let mut stream = FlightOptions::ticket(url, "x")
        .header("Authorization", "Bearer secret")
        .connect(|| {})
        .unwrap();
    assert_eq!(
        events(&mut stream).last().map(String::as_str),
        Some("finished")
    );
    assert_eq!(*tokens.lock().unwrap(), ["Bearer secret"]);

    let error = FlightOptions::ticket("grpc://localhost:1", "x")
        .header("bad header", "x")
        .connect(|| {})
        .unwrap_err();
    assert!(matches!(error, RequestError::InvalidHeader(_)), "{error}");
}

#[test]
fn a_failure_without_retry_ends_the_stream() {
    let url = serve(Service {
        do_get: Arc::new(|_, _| Err(Status::permission_denied("no access to plant 3"))),
        get_info: no_info(),
    });
    let mut stream = FlightOptions::ticket(url, "x").connect(|| {}).unwrap();
    let seen = events(&mut stream);
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert!(seen[0].contains("no access to plant 3"), "{seen:?}");
    assert_eq!(stream.wait_next(WAIT).map(|e| describe(e).0), None);
    assert!(stream.is_finished());
}

#[test]
fn a_failed_read_is_asked_for_again_with_growing_waits() {
    let url = serve(Service {
        do_get: Arc::new(|_, _| Err(Status::unavailable("busy"))),
        get_info: no_info(),
    });
    let mut stream = FlightOptions::ticket(url, "x")
        .retry(Duration::from_millis(10), Duration::from_millis(40))
        .connect(|| {})
        .unwrap();
    let mut waits = Vec::new();
    while waits.len() < 4 {
        match stream.wait_next(WAIT) {
            Some(FlightEvent::Retrying { retry_in, reason }) => {
                assert!(reason.contains("busy"), "{reason}");
                waits.push(retry_in.as_millis());
            }
            Some(other) => panic!("{other:?}"),
            None => panic!("nothing within {WAIT:?}"),
        }
    }
    assert_eq!(waits, [10, 20, 40, 40]);
}

#[test]
fn a_read_that_delivered_rows_starts_the_waits_over() {
    // Each read hands over a batch, then fails.
    let url = serve(Service {
        do_get: Arc::new(|_, _| {
            let failure = futures::stream::iter([Err(Status::unavailable("cut off"))]);
            Ok(encoded(vec![rows(0..1)]).chain(failure).boxed())
        }),
        get_info: no_info(),
    });
    let mut stream = FlightOptions::ticket(url, "x")
        .retry(Duration::from_millis(10), Duration::from_secs(1))
        .connect(|| {})
        .unwrap();
    let mut waits = Vec::new();
    let mut batches = 0;
    while waits.len() < 3 {
        match stream.wait_next(WAIT) {
            Some(FlightEvent::Retrying { retry_in, .. }) => waits.push(retry_in.as_millis()),
            Some(FlightEvent::Batch(_)) => batches += 1,
            Some(_) => {}
            None => panic!("nothing within {WAIT:?}"),
        }
    }
    assert_eq!(waits, [10, 10, 10]);
    assert!(batches >= 3, "each try starts the result over");
}

#[test]
fn a_service_that_is_not_there_fails() {
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut stream = FlightOptions::ticket(format!("grpc://127.0.0.1:{port}"), "x")
        .connect(|| {})
        .unwrap();
    let seen = events(&mut stream);
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert!(seen[0].contains("cannot connect"), "{seen:?}");
}

#[test]
fn urls_that_are_not_flight_urls_are_refused() {
    for url in [
        "ws://example.com/",
        "not a url",
        "grpc:///nohost",
        "grpc://:80",
    ] {
        let error = FlightOptions::ticket(url, "x").connect(|| {}).unwrap_err();
        assert!(
            matches!(error, RequestError::InvalidUrl(_)),
            "{url}: {error}"
        );
        assert!(error.to_string().starts_with("invalid Flight URL"));
    }
    #[cfg(not(feature = "tls"))]
    {
        let error = FlightOptions::ticket("grpc+tls://example.com", "x")
            .connect(|| {})
            .unwrap_err();
        assert!(matches!(error, RequestError::TlsDisabled));
    }
}

#[cfg(feature = "tls")]
#[test]
fn a_tls_url_opens_with_a_tls_handshake() {
    // A listener that only looks at the first byte the client sends: 0x16 opens a TLS
    // handshake record (an HTTP/2 preface would start with 'P').
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (first_tx, first_rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        if let Ok((mut socket, _)) = listener.accept() {
            let mut byte = [0_u8; 1];
            let _ = std::io::Read::read_exact(&mut socket, &mut byte);
            let _ = first_tx.send(byte[0]);
        }
    });
    let _stream = FlightOptions::ticket(format!("grpc+tls://127.0.0.1:{port}"), "x")
        .connect(|| {})
        .unwrap();
    assert_eq!(first_rx.recv_timeout(WAIT).unwrap(), 0x16);
}

#[test]
fn a_message_over_the_limit_fails_the_read() {
    let url = serve(Service {
        do_get: Arc::new(|_, _| Ok(encoded(vec![rows(0..1_000)]))),
        get_info: no_info(),
    });
    let mut stream = FlightOptions::ticket(url, "x")
        .max_message_bytes(1_024)
        .connect(|| {})
        .unwrap();
    let seen = events(&mut stream);
    assert!(
        seen.last().is_some_and(|e| e.starts_with("failed")),
        "{seen:?}"
    );
    assert!(!seen.iter().any(|e| e.starts_with("batch")), "{seen:?}");
}

/// Set when the service's stream is dropped: the client went away.
struct Dropped(Arc<AtomicBool>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn dropping_the_stream_ends_a_stream_that_never_ends() {
    let gone = Arc::new(AtomicBool::new(false));
    let sent = Arc::new(AtomicUsize::new(0));
    let (flag, count) = (gone.clone(), sent.clone());
    let url = serve(Service {
        do_get: Arc::new(move |_, _| {
            let guard = Dropped(flag.clone());
            let count = count.clone();
            let endless = futures::stream::unfold(0, move |i| {
                let _ = &guard;
                count.fetch_add(1, Ordering::SeqCst);
                async move {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    Some((Ok(rows(i..i + 1)), i + 1))
                }
            });
            let data = FlightDataEncoderBuilder::new()
                .with_schema(schema())
                .build(endless)
                .map_err(Status::from)
                .boxed();
            Ok(data)
        }),
        get_info: no_info(),
    });
    let mut stream = FlightOptions::ticket(url, "live").connect(|| {}).unwrap();
    let mut batches = 0;
    while batches < 5 {
        if let Some(FlightEvent::Batch(_)) = stream.wait_next(WAIT) {
            batches += 1;
        }
    }
    drop(stream);
    let until = Instant::now() + WAIT;
    while !gone.load(Ordering::SeqCst) && Instant::now() < until {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(gone.load(Ordering::SeqCst), "the service saw the client go");
}

#[test]
fn a_panic_on_the_reading_thread_fails_the_stream() {
    let mut stream = FlightOptions::ticket(by_ticket(), "t1")
        .connect(|| panic!("a waker that fails"))
        .unwrap();
    let seen = events(&mut stream);
    assert!(
        seen.last().is_some_and(|e| e.contains("panicked")),
        "{seen:?}"
    );
    assert_eq!(stream.wait_next(WAIT).map(|e| describe(e).0), None);
    assert!(stream.is_finished());
}

#[test]
fn a_longest_wait_shorter_than_the_first_is_the_first() {
    let url = serve(Service {
        do_get: Arc::new(|_, _| Err(Status::unavailable("busy"))),
        get_info: no_info(),
    });
    let mut stream = FlightOptions::ticket(url, "x")
        .retry(Duration::from_millis(30), Duration::from_millis(5))
        .connect(|| {})
        .unwrap();
    let mut waits = Vec::new();
    while waits.len() < 2 {
        if let Some(FlightEvent::Retrying { retry_in, .. }) = stream.wait_next(WAIT) {
            waits.push(retry_in.as_millis());
        }
    }
    assert_eq!(waits, [30, 30]);
}

#[test]
fn settings_out_of_range_still_read() {
    let mut stream = FlightOptions::ticket(by_ticket(), "t1")
        .max_message_bytes(0)
        .connect(|| {})
        .unwrap();
    assert_eq!(
        events(&mut stream),
        ["schema sensor,celsius", "batch [0.0]", "finished"]
    );
    assert!(stream.is_finished(), "as soon as the end is taken");
}

#[test]
fn failures_that_would_repeat_are_not_retried() {
    for status in [
        Status::permission_denied("no access"),
        Status::invalid_argument("bad query"),
        Status::unauthenticated("who are you"),
        Status::not_found("no such table"),
    ] {
        let code = status.code();
        let message = status.message().to_owned();
        let url = serve(Service {
            do_get: Arc::new(move |_, _| Err(Status::new(code, message.clone()))),
            get_info: no_info(),
        });
        let mut stream = FlightOptions::ticket(url, "x")
            .retry(Duration::from_millis(10), Duration::from_millis(10))
            .connect(|| {})
            .unwrap();
        let seen = events(&mut stream);
        assert_eq!(seen.len(), 1, "{code:?}: {seen:?}");
        assert!(seen[0].starts_with("failed"), "{code:?}: {seen:?}");
    }
}

#[test]
fn an_unreachable_location_falls_back_to_the_next() {
    let other = by_ticket();
    let closed = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let url = serve(Service {
        do_get: Arc::new(|_, _| Err(Status::not_found("not here"))),
        get_info: Arc::new(move |_| {
            Ok(FlightInfo::new().with_endpoint(
                FlightEndpoint::new()
                    .with_ticket(Ticket::new("t1"))
                    .with_location(format!("grpc://127.0.0.1:{closed}"))
                    .with_location(other.clone()),
            ))
        }),
    });
    let mut stream = FlightOptions::path(url, ["x"]).connect(|| {}).unwrap();
    assert_eq!(
        events(&mut stream),
        ["schema sensor,celsius", "batch [0.0]", "finished"]
    );
}

#[test]
fn headers_stay_with_the_service_they_were_given_for() {
    let tokens = Arc::new(Mutex::new(Vec::new()));
    let seen = tokens.clone();
    let other = serve(Service {
        do_get: Arc::new(move |_, metadata| {
            let token = metadata.get("authorization").and_then(|v| v.to_str().ok());
            seen.lock().unwrap().push(token.map(str::to_owned));
            Ok(encoded(Vec::new()))
        }),
        get_info: no_info(),
    });
    let url = serve(Service {
        do_get: Arc::new(|_, _| Err(Status::not_found("not here"))),
        get_info: Arc::new(move |_| {
            let endpoint = FlightEndpoint::new()
                .with_ticket(Ticket::new("x"))
                .with_location(other.clone());
            Ok(FlightInfo::new().with_endpoint(endpoint))
        }),
    });
    let mut stream = FlightOptions::path(url, ["x"])
        .header("authorization", "Bearer secret")
        .connect(|| {})
        .unwrap();
    assert_eq!(
        events(&mut stream).last().map(String::as_str),
        Some("finished")
    );
    assert_eq!(
        *tokens.lock().unwrap(),
        [None],
        "another port is another service"
    );
}

#[test]
fn endpoints_whose_columns_differ_fail_the_result() {
    let url = serve(Service {
        do_get: Arc::new(|ticket, _| {
            if ticket == b"a" {
                return Ok(encoded(vec![rows(0..1)]));
            }
            let other = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)]));
            let batch = RecordBatch::try_new(
                other.clone(),
                vec![Arc::new(StringArray::from(vec!["?"])) as ArrayRef],
            )
            .unwrap();
            let data = FlightDataEncoderBuilder::new()
                .with_schema(other)
                .build(futures::stream::iter([Ok(batch)]))
                .map_err(Status::from)
                .boxed();
            Ok(data)
        }),
        get_info: Arc::new(|_| {
            Ok(FlightInfo::new()
                .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new("a")))
                .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new("b"))))
        }),
    });
    let mut stream = FlightOptions::path(url, ["x"])
        .retry(Duration::from_millis(10), Duration::from_millis(10))
        .connect(|| {})
        .unwrap();
    let seen = events(&mut stream);
    assert_eq!(seen.len(), 3, "{seen:?}");
    assert!(
        seen[2].starts_with("failed") && seen[2].contains("columns differ"),
        "{seen:?}"
    );
}

#[test]
fn each_try_starts_the_result_and_its_numbers_over() {
    let url = serve(Service {
        do_get: Arc::new(|_, _| {
            let failure = futures::stream::iter([Err(Status::unavailable("cut off"))]);
            Ok(encoded(vec![rows(0..2)]).chain(failure).boxed())
        }),
        get_info: no_info(),
    });
    let mut stream = FlightOptions::ticket(url, "x")
        .row_numbers("row")
        .retry(Duration::from_millis(10), Duration::from_millis(10))
        .connect(|| {})
        .unwrap();
    let (mut schemas, mut firsts) = (0, Vec::new());
    while firsts.len() < 3 {
        match stream.wait_next(WAIT) {
            Some(FlightEvent::Schema(_)) => schemas += 1,
            Some(FlightEvent::Batch(batch)) => {
                firsts.push(batch.column(2).as_primitive::<UInt64Type>().value(0));
            }
            Some(_) => {}
            None => panic!("nothing within {WAIT:?}"),
        }
    }
    assert_eq!(firsts, [0, 0, 0]);
    assert!(schemas >= 3, "{schemas}");
}

#[test]
fn dropping_the_stream_stops_a_call_that_never_answers() {
    let wakes = Arc::new(AtomicUsize::new(0));
    let counted = wakes.clone();
    let url = serve(Service {
        do_get: Arc::new(|_, _| Err(Status::internal("unused"))),
        get_info: Arc::new(|_| {
            // Holds this call for good (the service runs it on a worker of its own).
            thread::sleep(Duration::from_secs(3_600));
            Err(Status::internal("woke"))
        }),
    });
    let stream = FlightOptions::path(url, ["slow"])
        .connect(move || {
            counted.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    thread::sleep(Duration::from_millis(300));
    assert_eq!(wakes.load(Ordering::SeqCst), 0, "no event yet");
    drop(stream);
    let until = Instant::now() + WAIT;
    while wakes.load(Ordering::SeqCst) == 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        1,
        "the thread ended (its last wake)"
    );
}

#[test]
fn dropping_a_stream_whose_queue_is_full_ends_it() {
    let gone = Arc::new(AtomicBool::new(false));
    let flag = gone.clone();
    let url = serve(Service {
        do_get: Arc::new(move |_, _| {
            let guard = Dropped(flag.clone());
            let endless = futures::stream::unfold(0, move |i| {
                let _ = &guard;
                async move { Some((Ok(rows(i..i + 1)), i + 1)) }
            });
            let data = FlightDataEncoderBuilder::new()
                .with_schema(schema())
                .build(endless)
                .map_err(Status::from)
                .boxed();
            Ok(data)
        }),
        get_info: no_info(),
    });
    let stream = FlightOptions::ticket(url, "live").connect(|| {}).unwrap();
    // Nothing is taken: the queue fills and the reading waits.
    thread::sleep(Duration::from_millis(1_000));
    drop(stream);
    let until = Instant::now() + WAIT;
    while !gone.load(Ordering::SeqCst) && Instant::now() < until {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(gone.load(Ordering::SeqCst), "the service saw the client go");
}

#[test]
fn urls_with_credentials_are_refused_and_headers_are_not_printed() {
    let error = FlightOptions::ticket("grpc://user:pass@localhost:1", "x")
        .connect(|| {})
        .unwrap_err();
    assert!(matches!(error, RequestError::InvalidUrl(_)), "{error}");
    assert!(!error.to_string().contains("pass"), "{error}");
    let options =
        FlightOptions::ticket("grpc://localhost:1", "x").header("authorization", "Bearer secret");
    let printed = format!("{options:?}");
    assert!(
        printed.contains("authorization") && !printed.contains("secret"),
        "{printed}"
    );
}

#[test]
fn failures_that_may_pass_are_retried() {
    for code in [
        tonic::Code::Unavailable,
        tonic::Code::ResourceExhausted,
        tonic::Code::Internal,
        tonic::Code::DeadlineExceeded,
    ] {
        let url = serve(Service {
            do_get: Arc::new(move |_, _| Err(Status::new(code, "for now"))),
            get_info: no_info(),
        });
        let mut stream = FlightOptions::ticket(url, "x")
            .retry(Duration::from_millis(10), Duration::from_millis(10))
            .connect(|| {})
            .unwrap();
        let first = stream.wait_next(WAIT).map(|event| describe(event).0);
        assert_eq!(first.as_deref(), Some("retrying 10"), "{code:?}");
    }
}

#[test]
fn endpoints_of_one_service_share_its_connection() {
    let (url, connections) = serve_counted(Service {
        do_get: Arc::new(|_, _| Ok(encoded(vec![rows(0..1)]))),
        get_info: Arc::new(|_| {
            Ok(FlightInfo::new()
                .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new("a")))
                .with_endpoint(
                    FlightEndpoint::new()
                        .with_ticket(Ticket::new("b"))
                        .with_location("arrow-flight-reuse-connection://?"),
                ))
        }),
    });
    let mut stream = FlightOptions::path(url, ["x"]).connect(|| {}).unwrap();
    assert_eq!(
        events(&mut stream).last().map(String::as_str),
        Some("finished")
    );
    assert_eq!(connections.load(Ordering::SeqCst), 1);
}

#[test]
fn empty_batches_are_not_handed_over() {
    let url = serve(Service {
        do_get: Arc::new(|_, _| {
            // Sent as they are: the encoder would leave empty batches out.
            let batches = [rows(0..0), rows(0..1), rows(1..1)];
            let data = arrow_flight::utils::batches_to_flight_data(&schema(), &batches)
                .map_err(|error| Status::internal(error.to_string()))?;
            Ok(futures::stream::iter(data.into_iter().map(Ok)).boxed())
        }),
        get_info: no_info(),
    });
    let mut stream = FlightOptions::ticket(url, "x").connect(|| {}).unwrap();
    assert_eq!(
        events(&mut stream),
        ["schema sensor,celsius", "batch [0.0]", "finished"]
    );
}

#[cfg(not(feature = "tls"))]
#[test]
fn an_endpoint_only_at_places_that_cannot_be_used_fails_without_retrying() {
    let url = serve(Service {
        do_get: Arc::new(|_, _| Err(Status::not_found("not here"))),
        get_info: Arc::new(|_| {
            Ok(FlightInfo::new().with_endpoint(
                FlightEndpoint::new()
                    .with_ticket(Ticket::new("t1"))
                    .with_location("grpc+tls://127.0.0.1:1"),
            ))
        }),
    });
    let mut stream = FlightOptions::path(url, ["x"])
        .retry(Duration::from_millis(10), Duration::from_millis(10))
        .connect(|| {})
        .unwrap();
    let seen = events(&mut stream);
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert!(seen[0].contains("needs the tls feature"), "{seen:?}");
}

#[tonic::async_trait]
impl FlightService for Service {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = Data;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = Data;

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        (self.get_info)(request.into_inner()).map(Response::new)
    }

    async fn do_get(&self, request: Request<Ticket>) -> Result<Response<Data>, Status> {
        let metadata = request.metadata().clone();
        (self.do_get)(&request.into_inner().ticket, &metadata).map(Response::new)
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
