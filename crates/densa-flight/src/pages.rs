//! A Flight SQL query's rows a page at a time, for densa's `PagedModel` (feature `paged`).

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Int32Type, Int64Type, UInt64Type};
use arrow_array::{Array, RecordBatch};
use arrow_flight::error::FlightError;
use arrow_flight::FlightClient;
use arrow_schema::{DataType, Schema, SchemaRef};
use densa_arrow::{PageError, PageQuery, PageSource};
use futures::TryStreamExt;
use tokio::runtime::Runtime;

use crate::client::{
    client, describe, headers, passing, places, statement, Headers, Place, Target,
};
use crate::options::FlightRequest;
use crate::{FlightOptions, RequestError};

impl FlightOptions {
    /// The rows of the Flight SQL query a page at a time, for a `densa_arrow::PagedModel`
    /// (feature `paged`): a result too large to hold, of which the grid holds only the pages it
    /// shows. Each page is a statement of its own the service runs, the query wrapped as
    /// `SELECT * FROM (query) AS densa_rows ORDER BY .. LIMIT .. OFFSET ..` (double-quoted
    /// names, `LIMIT` and `OFFSET`, as most SQL engines take them), and the count as
    /// `SELECT count(*) FROM (query) AS densa_rows`: so the query must be one `SELECT` with
    /// nothing after it, and its own `ORDER BY` is not kept (in SQL a derived table has no
    /// order): leave it out and sort the model (`GridModel::sort`, as a click on a title does).
    ///
    /// Connects and asks for the query's columns (wrapped with `LIMIT 0`) before returning
    /// ([`FlightPages::schema`]). Calls run on a thread of the source's own, each within
    /// [`call_timeout`](Self::call_timeout); the headers go with every call to the service, as
    /// for a stream; [`retry`](Self::retry) and [`row_numbers`](Self::row_numbers) do not
    /// apply.
    ///
    /// `keys` are columns that tell the result's rows apart (an id): each page is ordered by
    /// the columns the grid sorts by and then by them, so rows keep one order from page to
    /// page. Without keys the service decides the order, which may change between pages (rows
    /// showing twice, or not at all). A page deep in a large result costs the service a read of
    /// the result up to it, and an ordered page a sort of those rows.
    ///
    /// # Errors
    /// [`RequestError::NotSql`] unless made by [`FlightOptions::sql`];
    /// [`InvalidUrl`](RequestError::InvalidUrl), [`InvalidHeader`](RequestError::InvalidHeader)
    /// and [`TlsDisabled`](RequestError::TlsDisabled) as for [`connect`](Self::connect);
    /// [`Io`](RequestError::Io) if its thread cannot start; [`RequestError::Query`] if the
    /// service cannot be reached, does not answer in time or refuses the query, or a key is not
    /// one of its columns.
    pub fn paged(self, keys: &[&str]) -> Result<FlightPages, RequestError> {
        let FlightRequest::Sql(query) = &self.request else {
            return Err(RequestError::NotSql);
        };
        let origin = Target::origin(&self.url)?;
        let headers = headers(&self.headers)?;
        // A worker of its own keeps the connections alive between calls (pings answered).
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("densa-flight pages")
            .enable_all()
            .build()
            .map_err(RequestError::Io)?;
        let mut pages = FlightPages {
            runtime: Some(runtime),
            origin,
            headers,
            max_message: self.max_message,
            timeout: self.call_timeout,
            // A trailing `;` would end the statement inside the parentheses around it.
            query: query.trim_end().trim_end_matches(';').trim_end().to_owned(),
            keys: keys.iter().map(|&key| key.to_owned()).collect(),
            schema: Arc::new(Schema::empty()),
            clients: HashMap::new(),
        };
        let probe = format!("SELECT * FROM (\n{}\n) AS densa_rows LIMIT 0", pages.query);
        let (schema, _) = pages.run(&probe, false).map_err(|failure| {
            let mut reason = failure.reason;
            if failure.answered && !failure.passing {
                reason.push_str(
                    " (the query was sent wrapped as SELECT * FROM (query) AS densa_rows LIMIT 0: \
                     it must be one SELECT with nothing after it)",
                );
            }
            RequestError::Query(reason)
        })?;
        let schema =
            schema.ok_or_else(|| RequestError::Query("the service gave no columns".to_owned()))?;
        if let Some(key) = keys
            .iter()
            .find(|key| schema.column_with_name(key).is_none())
        {
            let columns: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            return Err(RequestError::Query(format!(
                "no column {key} to tell rows apart: give keys among {}",
                columns.join(", ")
            )));
        }
        pages.schema = schema;
        Ok(pages)
    }
}

/// The rows of a Flight SQL query a page at a time: a `densa_arrow::PageSource` made by
/// [`FlightOptions::paged`], for a `densa_arrow::PagedModel`. Its calls block the thread
/// calling them (the model's own) while a thread of the source's own makes them. Connections
/// are kept between calls; a call that fails on one kept from before is made once more on a
/// new one (the statements only read).
///
/// Its `Debug` output leaves header values out: they are often tokens.
pub struct FlightPages {
    /// `None` once dropped.
    runtime: Option<Runtime>,
    origin: Target,
    headers: Headers,
    max_message: usize,
    timeout: Duration,
    query: String,
    keys: Vec<String>,
    schema: SchemaRef,
    /// Connections by URI.
    clients: HashMap<String, FlightClient>,
}

/// Why a call failed.
struct Failure {
    reason: String,
    /// Whether asking again may help: a lost connection, a busy service.
    passing: bool,
    /// Whether the service answered with it (else the call did not reach it, or ran too long).
    answered: bool,
}

impl FlightPages {
    /// The result's columns, as the service gave them: give them to the model.
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn count_sql(&self) -> String {
        format!("SELECT count(*) FROM (\n{}\n) AS densa_rows", self.query)
    }

    fn page_sql(&self, query: &PageQuery, offset: u64, limit: usize) -> String {
        let order = query.order_by_sql(&self.keys);
        format!(
            "SELECT * FROM (\n{}\n) AS densa_rows {order} LIMIT {limit} OFFSET {offset}",
            self.query
        )
    }

    /// Runs `sql` at the service: the columns it named and the rows of every endpoint, in
    /// order (`ordered`: the endpoints must say they keep it). A call that fails in a way that
    /// may pass on a connection kept from before is made once more, anew.
    fn run(&mut self, sql: &str, ordered: bool) -> Result<Answer, Failure> {
        let reused = !self.clients.is_empty();
        match self.attempt(sql, ordered) {
            Err(failure) if reused && failure.passing => self.attempt(sql, ordered),
            result => result,
        }
    }

    /// One try of `sql`; a failure drops the connections.
    fn attempt(&mut self, sql: &str, ordered: bool) -> Result<Answer, Failure> {
        let timeout = self.timeout;
        let Some(runtime) = &self.runtime else {
            return Err(Failure {
                reason: "the pages' runtime has stopped".to_owned(),
                passing: false,
                answered: false,
            });
        };
        let call = Call {
            origin: &self.origin,
            headers: &self.headers,
            max_message: self.max_message,
            clients: &mut self.clients,
        };
        let timed = async move {
            match tokio::time::timeout(timeout, call.statement(sql, ordered)).await {
                Ok(answer) => answer,
                Err(_) => Err(Failure {
                    reason: format!(
                        "no answer within {timeout:?} (FlightOptions::call_timeout sets how long)"
                    ),
                    passing: false,
                    answered: false,
                }),
            }
        };
        let result = block_on(runtime, timed).unwrap_or_else(|| {
            Err(Failure {
                reason: "the call panicked".to_owned(),
                passing: false,
                answered: false,
            })
        });
        if result.is_err() {
            self.clients.clear();
        }
        result
    }
}

impl Drop for FlightPages {
    fn drop(&mut self) {
        // Without waiting for its thread: the pages may be dropped in async code.
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

/// The columns a statement named and the rows it gave.
type Answer = (Option<SchemaRef>, Vec<RecordBatch>);

/// Runs `future` on `runtime` from a thread of its own, so the calling thread may be inside an
/// async runtime (where blocking on another would panic). `None` if it panicked.
fn block_on<F>(runtime: &Runtime, future: F) -> Option<F::Output>
where
    F: Future + Send,
    F::Output: Send,
{
    std::thread::scope(|scope| scope.spawn(|| runtime.block_on(future)).join()).ok()
}

/// One call's view of a page source's connections.
struct Call<'a> {
    origin: &'a Target,
    headers: &'a Headers,
    max_message: usize,
    clients: &'a mut HashMap<String, FlightClient>,
}

/// A failure the service answered with.
fn failed(what: &str, error: &FlightError) -> Failure {
    let passing = match error {
        FlightError::Tonic(status) => passing(status.code()),
        FlightError::ExternalError(_) => true,
        _ => false,
    };
    Failure {
        reason: describe(what, error),
        passing,
        answered: true,
    }
}

impl Call<'_> {
    /// `GetFlightInfo` of the statement `sql`, then `DoGet` of each endpoint.
    async fn statement(mut self, sql: &str, ordered: bool) -> Result<Answer, Failure> {
        let origin = self.origin.clone();
        self.connect(&origin).await?;
        let mut info = self
            .client(&origin.uri)?
            .get_flight_info(statement(sql))
            .await
            .map_err(|error| failed("GetFlightInfo failed", &error))?;
        let endpoints = std::mem::take(&mut info.endpoint);
        if ordered && endpoints.len() > 1 && !info.ordered {
            return Err(Failure {
                reason: format!(
                    "the service gave the page in {} parts it does not keep in order \
                     (FlightInfo.ordered is false), and a page's rows must come in order",
                    endpoints.len()
                ),
                passing: false,
                answered: true,
            });
        }
        let mut schema = Schema::try_from(info).ok().map(Arc::new);
        let mut batches = Vec::new();
        for endpoint in endpoints {
            let ticket = endpoint.ticket.ok_or_else(|| Failure {
                reason: "an endpoint without a ticket".to_owned(),
                passing: false,
                answered: true,
            })?;
            let uri = self
                .reach(places(&endpoint.location).map_err(|reason| Failure {
                    reason,
                    passing: false,
                    answered: true,
                })?)
                .await?;
            let mut rows = self
                .client(&uri)?
                .do_get(ticket)
                .await
                .map_err(|error| failed("DoGet failed", &error))?;
            while let Some(batch) = rows
                .try_next()
                .await
                .map_err(|error| failed("reading failed", &error))?
            {
                if batch.num_rows() > 0 {
                    batches.push(batch);
                }
            }
            if schema.is_none() {
                schema = rows.schema().cloned();
            }
        }
        Ok((schema, batches))
    }

    /// Connects to the first of `places` that answers (none in the clear after TLS); the URI
    /// of its connection.
    async fn reach(&mut self, places: Vec<Place>) -> Result<String, Failure> {
        let mut last = Failure {
            reason: String::new(),
            passing: false,
            answered: false,
        };
        for place in places {
            let target = match place {
                Place::Origin => self.origin.clone(),
                Place::At(target) => match target.refused_after(self.origin) {
                    Some(reason) => {
                        last.reason = reason;
                        continue;
                    }
                    None => target,
                },
            };
            match self.connect(&target).await {
                Ok(()) => return Ok(target.uri),
                Err(failure) => last = failure,
            }
        }
        Err(last)
    }

    /// Makes sure there is a connection to `target`.
    async fn connect(&mut self, target: &Target) -> Result<(), Failure> {
        if self.clients.contains_key(&target.uri) {
            return Ok(());
        }
        let endpoint = target.endpoint().map_err(|reason| Failure {
            reason,
            passing: false,
            answered: false,
        })?;
        let channel = endpoint.connect().await.map_err(|error| Failure {
            reason: describe(&format!("cannot connect to {}", target.uri), &error),
            passing: true,
            answered: false,
        })?;
        let client = client(channel, target, self.origin, self.headers, self.max_message);
        self.clients.insert(target.uri.clone(), client);
        Ok(())
    }

    fn client(&mut self, uri: &str) -> Result<&mut FlightClient, Failure> {
        self.clients.get_mut(uri).ok_or_else(|| Failure {
            reason: "a connection went missing".to_owned(),
            passing: false,
            answered: false,
        })
    }
}

impl PageSource for FlightPages {
    fn row_count(&mut self, _: &PageQuery) -> Result<u64, PageError> {
        let sql = self.count_sql();
        let (_, batches) = self.run(&sql, false).map_err(|failure| failure.reason)?;
        Ok(count_of(&batches)?)
    }

    fn page(
        &mut self,
        query: &PageQuery,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<RecordBatch>, PageError> {
        let sql = self.page_sql(query, offset, limit);
        Ok(self.run(&sql, true).map_err(|failure| failure.reason)?.1)
    }

    /// Columns of nested types (lists, structs, maps) are not sorted by.
    fn can_sort(&self, column: &str) -> bool {
        self.schema
            .column_with_name(column)
            .is_some_and(|(_, field)| !field.data_type().is_nested())
    }
}

impl fmt::Debug for FlightPages {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let headers: Vec<String> = self
            .headers
            .iter()
            .map(|(name, _)| format!("{name}: <hidden>"))
            .collect();
        f.debug_struct("FlightPages")
            .field("url", &self.origin.uri)
            .field("query", &self.query)
            .field("keys", &self.keys)
            .field("headers", &headers)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

/// The count a `count(*)` query gave: the first value of the first column.
fn count_of(batches: &[RecordBatch]) -> Result<u64, String> {
    let column = batches
        .iter()
        .find(|batch| batch.num_rows() > 0 && batch.num_columns() > 0)
        .map(|batch| batch.column(0))
        .ok_or_else(|| "the count gave no rows".to_owned())?;
    let count = match column.data_type() {
        _ if column.is_null(0) => None,
        DataType::Int64 => u64::try_from(column.as_primitive::<Int64Type>().value(0)).ok(),
        DataType::UInt64 => Some(column.as_primitive::<UInt64Type>().value(0)),
        DataType::Int32 => u64::try_from(column.as_primitive::<Int32Type>().value(0)).ok(),
        DataType::Decimal128(_, 0) => {
            u64::try_from(column.as_primitive::<Decimal128Type>().value(0)).ok()
        }
        _ => None,
    };
    count.ok_or_else(|| format!("the count came as {}", column.data_type()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, Decimal128Array, Float64Array, Int32Array, Int64Array, RecordBatch, UInt64Array,
    };

    use super::count_of;

    fn one(column: ArrayRef) -> Vec<RecordBatch> {
        vec![RecordBatch::try_from_iter([("count", column)]).unwrap()]
    }

    #[test]
    fn a_count_is_read_from_the_integer_types_services_give_it_in() {
        assert_eq!(count_of(&one(Arc::new(Int64Array::from(vec![7])))), Ok(7));
        assert_eq!(count_of(&one(Arc::new(UInt64Array::from(vec![8])))), Ok(8));
        assert_eq!(count_of(&one(Arc::new(Int32Array::from(vec![9])))), Ok(9));
        let decimal = Decimal128Array::from(vec![10]).with_precision_and_scale(38, 0);
        assert_eq!(count_of(&one(Arc::new(decimal.unwrap()))), Ok(10));
        let fraction = Decimal128Array::from(vec![10]).with_precision_and_scale(38, 2);
        assert!(count_of(&one(Arc::new(fraction.unwrap()))).is_err());
        assert!(count_of(&one(Arc::new(Int64Array::from(vec![-1])))).is_err());
        assert!(count_of(&one(Arc::new(Int64Array::from(vec![None])))).is_err());
        let error = count_of(&one(Arc::new(Float64Array::from(vec![3.0])))).unwrap_err();
        assert_eq!(error, "the count came as Float64");
        assert_eq!(count_of(&[]).unwrap_err(), "the count gave no rows");
    }
}
