//! CSV and Parquet files as Apache Arrow record batches, for densa and any other Arrow
//! consumer.
//!
//! [`open`] reads a file's schema (inferring a CSV file's column types from its first
//! records), then its rows in batches, as an iterator. [`Loader`] reads them on a thread of
//! its own, a few batches ahead, waking the interface as each one is ready, so a window
//! keeps drawing while a large file loads. A column of row numbers can be added
//! ([`OpenOptions::row_numbers`]), a key for a `densa_data::LiveTable` when the file has
//! none.
//!
//! A Parquet file too large to hold is read a page at a time instead (feature `paged`):
//! `OpenOptions::paged` gives a `FilePages`, a `densa_arrow::PageSource` for densa's
//! `PagedModel`, which reads only the rows the grid shows, from anywhere in the file, in the
//! file's own order.
//!
//! Batches use Arrow 60, as densa's crates do, so they go into a live table, a grid or a
//! chart without conversion. Parquet's compression codecs are features, all on by default:
//! `snappy`, `gzip`, `lz4`, `zstd` and `brotli` (zstd builds C code; turn off what your
//! files do not use).
//!
//! ```no_run
//! use densa_files::{Loader, OpenOptions};
//!
//! let reader = OpenOptions::new().row_numbers("row").open("requests.parquet")?;
//! let mut loader = Loader::spawn(reader, || { /* ask the window for a frame */ })?;
//! // Each frame: take what is ready.
//! while let Some(batch) = loader.try_next() {
//!     let batch = batch?;
//!     println!("{} rows", batch.num_rows());
//! }
//! # Ok::<(), densa_files::FileError>(())
//! ```

mod error;
mod loader;
#[cfg(feature = "paged")]
mod pages;
mod reader;

pub use error::FileError;
pub use loader::Loader;
#[cfg(feature = "paged")]
pub use pages::FilePages;
pub use reader::{open, FileFormat, FileReader, OpenOptions, MAX_BATCH_ROWS};
