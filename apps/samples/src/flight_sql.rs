//! A local Flight SQL service that runs the statements it is given (feature `flight-sql`):
//! DataFusion over the web requests, for the paged demos and their tests.

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::{Any, CommandStatementQuery};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use arrow_schema::SchemaRef;
use densa_datafusion::{Engine, QueryEvent};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::flight::{serve, BATCH};

/// The longest a statement may run.
const STATEMENT_TIME: Duration = Duration::from_secs(60);

/// Starts a Flight SQL service on a free local port, on a thread of its own, that runs each
/// statement with DataFusion over the [`REQUESTS`](crate::REQUESTS) web requests, as table
/// `requests`, and returns its URL (`grpc://127.0.0.1:PORT`). `GetFlightInfo` of a statement
/// plans it, answering with its columns and one endpoint; `DoGet` of that runs it. Statements
/// only read. It runs until the process ends.
///
/// # Errors
/// If no local port can be had, the runtimes cannot start, or the rows cannot be made.
pub fn flight_sql_server() -> Result<String, Box<dyn Error>> {
    flight_sql_server_of(crate::REQUESTS)
}

/// [`flight_sql_server`] over the first `rows` requests (for tests).
///
/// # Errors
/// If no local port can be had, the runtimes cannot start, or the rows cannot be made.
pub fn flight_sql_server_of(rows: i64) -> Result<String, Box<dyn Error>> {
    let engine = Engine::with_threads(2)?;
    let rows = rows.max(0);
    let schema = crate::requests(0, 0)?.schema();
    let batches = (0..rows)
        .step_by(BATCH as usize)
        .map(|first| crate::requests(first, (first + BATCH).min(rows)))
        .collect::<Result<Vec<_>, _>>()?;
    engine.register_batches("requests", &schema, &batches)?;
    serve(Statements {
        engine: Arc::new(engine),
    })
}

/// Runs the statements it is given.
struct Statements {
    engine: Arc<Engine>,
}

/// The query of a `GetFlightInfo` of a Flight SQL statement.
fn statement(descriptor: &FlightDescriptor) -> Result<String, Status> {
    use prost::Message;
    Any::decode(&*descriptor.cmd)
        .ok()
        .and_then(|any| any.unpack::<CommandStatementQuery>().ok().flatten())
        .map(|command| command.query)
        .ok_or_else(|| Status::unimplemented("only Flight SQL statements"))
}

/// Runs `query` to its end: its columns and rows, or why it failed.
fn run(engine: &Engine, query: &str) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
    let mut stream = engine.sql(query).run(|| {});
    let mut schema = None;
    let mut batches = Vec::new();
    loop {
        match stream.wait_next(STATEMENT_TIME) {
            Some(QueryEvent::Schema(columns)) => schema = Some(columns),
            Some(QueryEvent::Batch(batch)) => batches.push(batch),
            Some(QueryEvent::Finished) => {
                let schema = schema.ok_or("the statement gave no columns")?;
                return Ok((schema, batches));
            }
            Some(QueryEvent::Failed { reason }) => return Err(reason),
            Some(_) => {}
            None if stream.is_finished() => return Err("the statement stopped".to_owned()),
            None => return Err(format!("the statement ran over {STATEMENT_TIME:?}")),
        }
    }
}

#[tonic::async_trait]
impl FlightService for Statements {
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
        let query = statement(&request.into_inner())?;
        let engine = Arc::clone(&self.engine);
        let planned = query.clone();
        let schema = tokio::task::spawn_blocking(move || engine.schema(&planned))
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(query));
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|error| Status::internal(error.to_string()))?
            .with_endpoint(endpoint);
        Ok(Response::new(info))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let query = String::from_utf8(request.into_inner().ticket.to_vec())
            .map_err(|_| Status::invalid_argument("a ticket is a statement"))?;
        let engine = Arc::clone(&self.engine);
        let (schema, batches) = tokio::task::spawn_blocking(move || run(&engine, &query))
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(Status::invalid_argument)?;
        let batches = futures::stream::iter(batches.into_iter().map(Ok));
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
