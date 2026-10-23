//! A local Arrow Flight service for the apps' demos and tests: any request's answer is the
//! million web requests (a service log) in four endpoints, made as they are sent, except
//! Flight SQL's list of tables, which names them: `demo.public.requests`.

use std::error::Error;
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::{Any, CommandGetTables};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

const ENDPOINTS: i64 = 4;
const BATCH: i64 = 16_384;
/// The ticket of the list of tables.
const TABLES: &str = "tables";

/// Starts a Flight service on a free local port, on a thread of its own, and returns its URL
/// (`grpc://127.0.0.1:PORT`). Whatever is asked (a path, a command, a Flight SQL query), it
/// answers with the [`REQUESTS`](crate::REQUESTS) web requests, a quarter per endpoint (the
/// ticket is the quarter's index, `0` to `3`). It runs until the process ends.
///
/// # Errors
/// If no local port can be had or the runtime cannot start.
pub fn flight_server() -> Result<String, Box<dyn Error>> {
    flight_server_of(crate::REQUESTS)
}

/// [`flight_server`] answering with the first `rows` requests (for tests).
///
/// # Errors
/// If no local port can be had or the runtime cannot start.
pub fn flight_server_of(rows: i64) -> Result<String, Box<dyn Error>> {
    serve(Requests {
        rows,
        fail_second: AtomicBool::new(false),
    })
}

/// [`flight_server_of`] whose second `DoGet` (the first of the second quarter) fails once, as
/// a service that is briefly unavailable does: a client that asks again gets the rest (for
/// tests of starting a result over).
///
/// # Errors
/// If no local port can be had or the runtime cannot start.
pub fn flight_server_failing_once(rows: i64) -> Result<String, Box<dyn Error>> {
    serve(Requests {
        rows,
        fail_second: AtomicBool::new(true),
    })
}

fn serve(requests: Requests) -> Result<String, Box<dyn Error>> {
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
                    .add_service(FlightServiceServer::new(requests))
                    .serve_with_incoming(incoming)
                    .await;
                if let Err(error) = served {
                    eprintln!("demo service stopped: {error}");
                }
            });
        })?;
    Ok(format!("grpc://{address}"))
}

/// Whether `descriptor` is Flight SQL's `CommandGetTables`.
fn asks_for_tables(descriptor: &FlightDescriptor) -> bool {
    use prost::Message;
    Any::decode(&*descriptor.cmd)
        .ok()
        .and_then(|any| any.unpack::<CommandGetTables>().ok().flatten())
        .is_some()
}

/// The service's one table, in the columns Flight SQL lists tables in.
fn tables() -> Result<RecordBatch, arrow_schema::ArrowError> {
    let text = |value: &str| Arc::new(StringArray::from(vec![value])) as ArrayRef;
    RecordBatch::try_from_iter([
        ("catalog_name", text("demo")),
        ("db_schema_name", text("public")),
        ("table_name", text("requests")),
        ("table_type", text("TABLE")),
    ])
}

/// Answers every query with the requests, a quarter per endpoint (the ticket is its index).
struct Requests {
    rows: i64,
    /// Whether the second quarter's first `DoGet` is still to fail.
    fail_second: AtomicBool,
}

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
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        if asks_for_tables(&request.into_inner()) {
            let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(TABLES));
            return Ok(Response::new(FlightInfo::new().with_endpoint(endpoint)));
        }
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
        if ticket == TABLES.as_bytes() {
            let tables = futures::stream::iter([tables().map_err(Into::into)]);
            let data = FlightDataEncoderBuilder::new()
                .build(tables)
                .map_err(Status::from)
                .boxed();
            return Ok(Response::new(data));
        }
        let part: i64 = std::str::from_utf8(&ticket)
            .ok()
            .and_then(|text| text.parse().ok())
            .filter(|part| (0..ENDPOINTS).contains(part))
            .ok_or_else(|| Status::not_found("no such part"))?;
        // Relaxed: a flag for tests, guarding no other data.
        if part == 1 && self.fail_second.swap(false, Ordering::Relaxed) {
            return Err(Status::unavailable("briefly unavailable"));
        }
        let rows = self.rows.max(0);
        let (start, end) = (part * rows / ENDPOINTS, (part + 1) * rows / ENDPOINTS);
        let batches = futures::stream::iter((start..end).step_by(BATCH as usize))
            .map(move |first| crate::requests(first, (first + BATCH).min(end)).map_err(Into::into));
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

#[cfg(test)]
mod tests {
    use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
    use prost::Message;

    use super::*;

    #[test]
    fn the_list_of_tables_is_told_from_queries() {
        let tables = CommandGetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types: Vec::new(),
            include_schema: false,
        };
        let query = CommandStatementQuery {
            query: "SELECT 1".to_owned(),
            transaction_id: None,
        };
        let asked = |command: Vec<u8>| asks_for_tables(&FlightDescriptor::new_cmd(command));
        assert!(asked(tables.as_any().encode_to_vec()));
        assert!(!asked(query.as_any().encode_to_vec()));
        assert!(!asked(b"anything".to_vec()));
        let listed = super::tables().unwrap();
        let schema = listed.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            ["catalog_name", "db_schema_name", "table_name", "table_type"]
        );
    }
}
