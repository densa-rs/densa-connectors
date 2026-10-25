//! Arrow Flight and Flight SQL results as Apache Arrow record batches, for densa and any
//! other Arrow consumer.
//!
//! [`FlightOptions`] names a Flight service (`grpc://`, `grpc+tls://`) and what to ask it
//! for: a ticket, a dataset path, a command, a Flight SQL query or a Flight SQL service's
//! list of tables (the `sql` feature, on by default). [`FlightOptions::connect`] reads the result on a thread of its own, every
//! endpoint in order, and wakes the interface after each [`FlightEvent`]: the schema first,
//! then batches as they arrive, then the end.
//!
//! Batches use Arrow 60, as densa's crates do, so they go into a `densa_data::LiveTable`
//! with `upsert` as they arrive ([`FlightOptions::row_numbers`] adds a key when the result
//! has none). The thread runs a small tokio runtime of its own: apps need none. A stream
//! that never ends (live data served over `DoGet`) is read as it comes;
//! [`FlightOptions::retry`] asks again when one fails. TLS goes through rustls (the `tls`
//! feature, on by default).
//!
//! A Flight SQL result too large to hold goes in densa's grid a page at a time: with the
//! `paged` feature, `FlightOptions::paged` makes a `densa_arrow::PageSource` of a query, each
//! page a statement the service runs (`ORDER BY` the grid's sort and the keys given, `LIMIT`,
//! `OFFSET`), counted with `count(*)`.
//!
//! ```no_run
//! use densa_flight::{FlightEvent, FlightOptions};
//!
//! # #[cfg(feature = "sql")]
//! # fn main() -> Result<(), densa_flight::RequestError> {
//! let mut result = FlightOptions::sql("grpc://localhost:32010", "SELECT * FROM trips")
//!     .header("authorization", "Bearer <token>")
//!     .row_numbers("row")
//!     .connect(|| { /* ask the window for a frame */ })?;
//! // Each frame: take what has arrived.
//! while let Some(event) = result.try_next() {
//!     match event {
//!         FlightEvent::Schema(schema) => println!("{} columns", schema.fields().len()),
//!         FlightEvent::Batch(batch) => println!("{} rows", batch.num_rows()),
//!         FlightEvent::Failed { reason } => println!("{reason}"),
//!         _ => {}
//!     }
//! }
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "sql"))]
//! # fn main() {}
//! ```

mod client;
mod numbers;
mod options;
#[cfg(feature = "paged")]
mod pages;
mod stream;

pub use options::{FlightOptions, RequestError};
#[cfg(feature = "paged")]
pub use pages::FlightPages;
pub use stream::{FlightEvent, FlightStream};
