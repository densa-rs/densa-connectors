//! WebSocket streams as Apache Arrow record batches, for densa and any other Arrow consumer.
//!
//! [`StreamOptions`] names a `ws://` or `wss://` URL and what its messages hold: JSON rows
//! of a schema you give or read from the first message, or an Arrow IPC stream. [`StreamOptions::connect`] reads it on a
//! thread of its own, gathering JSON rows into batches (handing a partial one over after a
//! short delay, so a slow stream still shows promptly), reconnecting with growing waits when
//! the connection drops, and waking the interface after each [`StreamEvent`].
//!
//! Batches use Arrow 60, as densa's crates do, so they go into a `densa_data::LiveTable`
//! with `upsert` as they arrive. No async runtime: one thread per stream, with blocking
//! reads that time out so it can hand over rows and stop when dropped. `wss://` goes
//! through rustls (the `tls` feature, on by default).
//!
//! ```no_run
//! use std::sync::Arc;
//! use arrow_schema::{DataType, Field, Schema};
//! use densa_ws::{StreamEvent, StreamOptions};
//!
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("sensor", DataType::Utf8, false),
//!     Field::new("celsius", DataType::Float64, true),
//! ]));
//! let mut stream = StreamOptions::json("wss://example.com/readings", schema)
//!     .on_connect(r#"{"subscribe":"plant-3"}"#)
//!     .connect(|| { /* ask the window for a frame */ })?;
//! // Each frame: take what has arrived.
//! while let Some(event) = stream.try_next() {
//!     match event {
//!         StreamEvent::Batch(batch) => println!("{} rows", batch.num_rows()),
//!         StreamEvent::Disconnected { reason, retry_in } => println!("{reason}; again in {retry_in:?}"),
//!         _ => {}
//!     }
//! }
//! # Ok::<(), densa_ws::StreamError>(())
//! ```

mod decode;
mod options;
mod stream;

pub use options::{MessageFormat, StreamError, StreamOptions, MAX_BATCH_ROWS};
pub use stream::{Stream, StreamEvent};
