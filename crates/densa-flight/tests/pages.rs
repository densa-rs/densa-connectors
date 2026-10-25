//! A Flight SQL query read a page at a time (feature `paged`): the statements sent for the
//! columns, the count and each page in the grid's order, pages in several endpoints (only kept
//! in order), headers on every call, a call made once more after a passing failure, a time
//! limit, and what is refused before any page is read.
#![cfg(feature = "paged")]

use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::{Any, CommandStatementQuery};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use densa_arrow::{PageQuery, PageSource, PagedModel, SortKey};
use densa_flight::{FlightOptions, RequestError};
use densa_grid::{CellText, GridModel, Palette, SortOrder};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use tonic::metadata::MetadataMap;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

/// Rows of the table the service answers about.
const ROWS: i64 = 1_000;

type Data = BoxStream<'static, Result<FlightData, Status>>;
/// What a service was asked, each with the `authorization` header it came with.
type Log = Arc<Mutex<Vec<(String, Option<String>)>>>;

/// `id`, `name` (`n` and the id) and `tags` (an empty list: a column not sorted by).
fn schema() -> SchemaRef {
    let tags = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("tags", tags, false),
    ]))
}

fn rows(ids: &[i64]) -> RecordBatch {
    let mut tags = ListBuilder::new(StringBuilder::new());
    for _ in ids {
        tags.append(true);
    }
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                ids.iter().map(|id| format!("n{id}")),
            )),
            Arc::new(tags.finish()),
        ],
    )
    .unwrap()
}

fn count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count(*)",
        DataType::Int64,
        false,
    )]))
}

/// What the service answers `query` with: its columns and rows. It knows the statements a
/// page source sends: the count, and rows by `LIMIT` and `OFFSET`, by id either way.
fn answer(query: &str) -> Result<(SchemaRef, Vec<i64>), Status> {
    if query.contains("nowhere") {
        return Err(Status::invalid_argument("table nowhere not found"));
    }
    if query.contains("OFFSET 666") {
        return Err(Status::unavailable("the service is busy"));
    }
    if query.starts_with("SELECT count(*)") {
        return Ok((count_schema(), vec![ROWS]));
    }
    let (_, tail) = query
        .rsplit_once(" LIMIT ")
        .ok_or_else(|| Status::invalid_argument("no LIMIT"))?;
    let (limit, offset) = tail.split_once(" OFFSET ").unwrap_or((tail, "0"));
    let limit: i64 = limit.parse().map_err(|_| Status::invalid_argument(tail))?;
    let offset: i64 = offset.parse().map_err(|_| Status::invalid_argument(tail))?;
    let end = (offset + limit).min(ROWS);
    let mut ids: Vec<i64> = (offset.min(ROWS)..end).collect();
    if query.contains(r#"ORDER BY "id" DESC"#) {
        ids = ids.iter().map(|id| ROWS - 1 - id).collect();
    }
    Ok((schema(), ids))
}

/// A Flight SQL service answering statements as [`answer`] does, the rows of each in `parts`
/// endpoints (said to be in order if `ordered`). It logs each statement and each `DoGet` with
/// the `authorization` header it came with; a statement with `pg_sleep` in it is answered
/// after three seconds, and the first with `OFFSET 444` fails as a busy service does.
#[derive(Clone)]
struct Sql {
    statements: Log,
    gets: Log,
    parts: usize,
    ordered: bool,
    fail_once: Arc<AtomicBool>,
}

/// A service running on a local port.
struct Running {
    url: String,
    statements: Log,
    gets: Log,
    connections: Arc<AtomicUsize>,
}

/// A service of `parts` endpoints per answer, said to be in order if `ordered`.
fn serve(parts: usize, ordered: bool) -> Running {
    let service = Sql {
        statements: Log::default(),
        gets: Log::default(),
        parts,
        ordered,
        fail_once: Arc::new(AtomicBool::new(true)),
    };
    let (statements, gets) = (service.statements.clone(), service.gets.clone());
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
    Running {
        url: format!("grpc://{address}"),
        statements,
        gets,
        connections,
    }
}

fn service(parts: usize) -> Running {
    serve(parts, true)
}

fn statements(log: &Log) -> Vec<String> {
    log.lock()
        .unwrap()
        .iter()
        .map(|(sql, _)| sql.clone())
        .collect()
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect()
}

#[test]
fn a_query_is_read_a_page_at_a_time_by_the_statements_it_takes() {
    let service = service(1);
    let mut pages = FlightOptions::sql(&service.url, "SELECT * FROM t;")
        .header("authorization", "Bearer secret")
        .paged(&["id"])
        .unwrap();
    assert_eq!(pages.schema().fields().len(), 3);
    assert_eq!(pages.row_count(&PageQuery::new()).unwrap(), 1_000);
    assert_eq!(
        ids(&pages.page(&PageQuery::new(), 20, 10).unwrap()),
        (20..30).collect::<Vec<_>>()
    );
    let wrapped = "SELECT * FROM (\nSELECT * FROM t\n) AS densa_rows";
    assert_eq!(
        statements(&service.statements),
        [
            format!("{wrapped} LIMIT 0"),
            "SELECT count(*) FROM (\nSELECT * FROM t\n) AS densa_rows".to_owned(),
            format!(r#"{wrapped} ORDER BY "id" LIMIT 10 OFFSET 20"#),
        ]
    );
    for log in [&service.statements, &service.gets] {
        let log = log.lock().unwrap();
        assert!(!log.is_empty());
        assert!(
            log.iter()
                .all(|(_, header)| header.as_deref() == Some("Bearer secret")),
            "the header goes with every call: {log:?}"
        );
    }
    let shown = format!("{pages:?}");
    assert!(
        shown.contains("authorization: <hidden>") && !shown.contains("secret"),
        "{shown}"
    );
}

#[test]
fn a_sort_is_asked_of_the_service_with_the_keys_after() {
    let service = service(1);
    let mut pages = FlightOptions::sql(&service.url, "SELECT * FROM t")
        .paged(&["id"])
        .unwrap();
    let by_name = PageQuery::new().sorted_by(SortKey::new("name", SortOrder::Descending));
    pages.page(&by_name, 0, 5).unwrap();
    let by_id = PageQuery::new().sorted_by(SortKey::new("id", SortOrder::Descending));
    assert_eq!(ids(&pages.page(&by_id, 0, 3).unwrap()), [999, 998, 997]);
    let sent = statements(&service.statements);
    assert!(
        sent[1].ends_with(r#"ORDER BY "name" DESC, "id" LIMIT 5 OFFSET 0"#),
        "{sent:?}"
    );
    assert!(
        sent[2].ends_with(r#"ORDER BY "id" DESC LIMIT 3 OFFSET 0"#),
        "{sent:?}"
    );
    assert!(pages.can_sort("name") && !pages.can_sort("tags") && !pages.can_sort("nope"));
}

#[test]
fn a_page_in_several_endpoints_comes_together_only_in_their_order() {
    let mut pages = FlightOptions::sql(service(3).url, "SELECT * FROM t")
        .paged(&[])
        .unwrap();
    let page = pages.page(&PageQuery::new(), 100, 50).unwrap();
    assert_eq!(ids(&page), (100..150).collect::<Vec<_>>());
    // Parts the service does not keep in order: a count needs no order, a page does.
    let mut pages = FlightOptions::sql(serve(3, false).url, "SELECT * FROM t")
        .paged(&[])
        .unwrap();
    assert_eq!(pages.row_count(&PageQuery::new()).unwrap(), 1_000);
    let error = pages
        .page(&PageQuery::new(), 0, 50)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("3 parts it does not keep in order (FlightInfo.ordered is false)"),
        "{error}"
    );
}

#[test]
fn what_cannot_be_read_a_page_at_a_time_is_refused_saying_why() {
    let url = service(1).url;
    let refused = |options: FlightOptions, keys: &[&str]| match options.paged(keys) {
        Err(error) => error,
        Ok(pages) => panic!("{pages:?}"),
    };
    let error = refused(FlightOptions::ticket(&url, "t"), &[]);
    assert!(matches!(error, RequestError::NotSql));
    assert_eq!(
        error.to_string(),
        "only a Flight SQL query is read a page at a time: make the options with FlightOptions::sql"
    );
    let RequestError::Query(reason) =
        refused(FlightOptions::sql(&url, "SELECT * FROM t"), &["key"])
    else {
        panic!("a key not among the columns")
    };
    assert_eq!(
        reason,
        "no column key to tell rows apart: give keys among id, name, tags"
    );
    let RequestError::Query(reason) =
        refused(FlightOptions::sql(&url, "SELECT * FROM nowhere"), &[])
    else {
        panic!("a query the service refuses")
    };
    assert!(
        reason.contains("nowhere not found")
            && reason.contains("must be one SELECT with nothing after it"),
        "{reason}"
    );
    // A port nothing listens on: not a refusal, so no word about the query.
    let closed = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let RequestError::Query(reason) = refused(
        FlightOptions::sql(format!("grpc://{closed}"), "SELECT 1"),
        &[],
    ) else {
        panic!("a service that cannot be reached")
    };
    assert!(
        reason.contains("cannot connect") && !reason.contains("SELECT"),
        "{reason}"
    );
    assert!(matches!(
        refused(
            FlightOptions::sql("grpc://user@localhost:1", "SELECT 1"),
            &[]
        ),
        RequestError::InvalidUrl(_)
    ));
}

#[test]
fn a_call_that_fails_in_passing_is_made_once_more_on_a_new_connection() {
    let service = service(1);
    let mut pages = FlightOptions::sql(&service.url, "SELECT * FROM t")
        .paged(&["id"])
        .unwrap();
    let before = service.connections.load(Ordering::SeqCst);
    // The first OFFSET 444 fails as a busy service does; the call is made again, anew.
    let page = pages.page(&PageQuery::new(), 444, 2).unwrap();
    assert_eq!(ids(&page), [444, 445]);
    assert_eq!(service.connections.load(Ordering::SeqCst), before + 1);
    // One that keeps failing fails once more, then says why; the next call connects anew.
    let error = pages
        .page(&PageQuery::new(), 666, 10)
        .unwrap_err()
        .to_string();
    assert!(error.contains("the service is busy"), "{error}");
    assert_eq!(service.connections.load(Ordering::SeqCst), before + 2);
    assert_eq!(ids(&pages.page(&PageQuery::new(), 0, 2).unwrap()), [0, 1]);
    pages.page(&PageQuery::new(), 2, 2).unwrap();
    assert_eq!(
        service.connections.load(Ordering::SeqCst),
        before + 3,
        "connected anew, then kept"
    );
    // A refusal is not made again.
    let sent = statements(&service.statements).len();
    let refused = PageQuery::new().sorted_by(SortKey::new("nowhere", SortOrder::Ascending));
    assert!(pages.page(&refused, 0, 2).is_err());
    assert_eq!(statements(&service.statements).len(), sent + 1);
}

#[test]
fn a_call_past_its_time_fails_saying_so() {
    let url = service(1).url;
    let started = Instant::now();
    let error = FlightOptions::sql(url, "SELECT pg_sleep(10)")
        .call_timeout(Duration::from_millis(300))
        .paged(&[])
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("no answer within 300ms (FlightOptions::call_timeout sets how long)"),
        "{error}"
    );
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn pages_are_read_from_async_code_too() {
    let url = service(1).url;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let first = runtime.block_on(async move {
        let mut pages = FlightOptions::sql(url, "SELECT * FROM t")
            .paged(&["id"])
            .unwrap();
        let page = pages.page(&PageQuery::new(), 0, 3).unwrap();
        // Dropped here, in a task: without waiting for its thread.
        drop(pages);
        ids(&page)
    });
    assert_eq!(first, [0, 1, 2]);
}

#[test]
fn the_grid_model_pages_through_the_service() {
    let pages = FlightOptions::sql(service(2).url, "SELECT * FROM t")
        .paged(&["id"])
        .unwrap();
    let mut model = PagedModel::new(pages.schema(), pages, || {}).unwrap();
    let frames = |model: &mut PagedModel, done: &dyn Fn(&PagedModel) -> bool| {
        let start = Instant::now();
        while !done(model) {
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "{:?}",
                model.error()
            );
            model.prepare();
            model.showing(0..20);
            thread::sleep(Duration::from_millis(1));
        }
    };
    let cell = |model: &PagedModel, row: usize| {
        let mut text = CellText::new();
        model.cell(row, 1, &mut text, &Palette::new());
        text.as_str().to_owned()
    };
    frames(&mut model, &|m| m.is_fetched(19));
    assert_eq!(model.row_count(), Some(1_000));
    assert_eq!(cell(&model, 19), "n19");
    assert!(
        !model.sort(2, SortOrder::Ascending),
        "tags are not sorted by"
    );
    assert!(model.sort(0, SortOrder::Descending));
    frames(&mut model, &|m| m.is_fetched(0));
    assert_eq!(cell(&model, 0), "n999");
}

/// The `authorization` header in `metadata`.
fn authorization(metadata: &MetadataMap) -> Option<String> {
    metadata
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

#[tonic::async_trait]
impl FlightService for Sql {
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
        let header = authorization(request.metadata());
        let any = Any::decode(request.into_inner().cmd)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let command: CommandStatementQuery = any
            .unpack()
            .map_err(|error| Status::invalid_argument(error.to_string()))?
            .ok_or_else(|| Status::invalid_argument("not a statement"))?;
        let query = command.query;
        self.statements
            .lock()
            .unwrap()
            .push((query.clone(), header));
        if query.contains("pg_sleep") {
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        if query.contains("OFFSET 444") && self.fail_once.swap(false, Ordering::SeqCst) {
            return Err(Status::unavailable("briefly unavailable"));
        }
        let (schema, _) = answer(&query)?;
        let mut info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|error| Status::internal(error.to_string()))?
            .with_ordered(self.ordered);
        for part in 0..self.parts {
            let ticket = Ticket::new(format!("{part}|{query}"));
            info = info.with_endpoint(FlightEndpoint::new().with_ticket(ticket));
        }
        Ok(Response::new(info))
    }

    async fn do_get(&self, request: Request<Ticket>) -> Result<Response<Data>, Status> {
        let header = authorization(request.metadata());
        let ticket = String::from_utf8(request.into_inner().ticket.to_vec())
            .map_err(|_| Status::invalid_argument("ticket"))?;
        let (part, query) = ticket.split_once('|').unwrap();
        self.gets.lock().unwrap().push((query.to_owned(), header));
        let part: usize = part.parse().unwrap();
        let (schema, values) = answer(query)?;
        // Each endpoint its share, in order.
        let share = values.len().div_ceil(self.parts);
        let start = (part * share).min(values.len());
        let end = (start + share).min(values.len());
        let batch = if schema.fields().len() == 1 {
            let count: ArrayRef = Arc::new(Int64Array::from(values[start..end].to_vec()));
            RecordBatch::try_new(schema.clone(), vec![count]).unwrap()
        } else {
            rows(&values[start..end])
        };
        let batches = futures::stream::iter([Ok(batch)]);
        let data = FlightDataEncoderBuilder::new()
            .with_schema(schema)
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
