//! SQL over Arrow with Apache DataFusion, for densa and any other Arrow consumer.
//!
//! An [`Engine`] holds a DataFusion session and the tokio runtime its queries run on (apps
//! need none of their own, and may still have one). Register Parquet files, CSV files or
//! record batches as tables, then [`Engine::sql`] names a query and [`Query::run`] runs it
//! in the background, handing its result over as [`QueryEvent`]s: the schema first, then
//! batches as they are computed, then the end, waking the interface after each. Queries
//! only read unless [`Query::allow_changes`] is set.
//!
//! Batches use Arrow 60, as densa's crates do, so they go into a `densa_data::LiveTable`
//! with `upsert` as they arrive. DataFusion 55 is built on Arrow 59: batches cross between
//! the two as Arrow IPC streams, written by one version and read by the other (two copies
//! of each batch; string views are compacted first, so a filtered batch does not carry
//! the bytes of the rows it dropped).
//!
//! A result too large to hold goes in densa's grid a page at a time: with the `paged`
//! feature, `Engine::pages` makes a `densa_arrow::PageSource` of a query, each page a query
//! of its own (`ORDER BY` the grid's sort and the keys given, `LIMIT`, `OFFSET`), counted with
//! `count(*)`. [`Engine::schema`] gives a query's columns without running it.
//!
//! ```no_run
//! use densa_datafusion::{Engine, QueryEvent};
//!
//! let engine = Engine::new()?;
//! engine.register_parquet("requests", "requests.parquet")?;
//! let mut result = engine
//!     .sql("SELECT service, count(*) AS requests, avg(ms) AS ms FROM requests GROUP BY service")
//!     .row_numbers("row")
//!     .run(|| { /* ask the window for a frame */ });
//! // Each frame: take what has arrived.
//! while let Some(event) = result.try_next() {
//!     match event {
//!         QueryEvent::Batch(batch) => println!("{} rows", batch.num_rows()),
//!         QueryEvent::Failed { reason } => println!("{reason}"),
//!         _ => {}
//!     }
//! }
//! # Ok::<(), densa_datafusion::EngineError>(())
//! ```

mod bridge;
mod engine;
mod numbers;
#[cfg(feature = "paged")]
mod pages;

pub use engine::{Engine, EngineError, Query, QueryEvent, QueryStream};
#[cfg(feature = "paged")]
pub use pages::SqlPages;
