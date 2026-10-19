use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TryRecvError};
use std::thread;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::{FileError, FileReader};

/// Batches read ahead of the one taken: a slow taker holds the reader back rather than
/// letting batches pile up in memory.
const READ_AHEAD: usize = 4;

/// Reads a file on a thread of its own and hands its batches over as they are read, so an
/// interface keeps drawing while a large file loads.
///
/// The thread reads at most a few batches ahead of [`try_next`](Self::try_next), and calls
/// `wake` after each batch it hands over (and once when it ends): give it something cheap
/// that asks the interface for a frame, such as `densa_window::Waker::wake`. A panic while
/// reading (a decoder's, or `wake`'s) is handed over as an error, never mistaken for the end
/// of the file. Dropping the loader stops the thread at its next batch; a read that blocks
/// (a pipe, a hung network share) keeps it until the read returns.
#[derive(Debug)]
pub struct Loader {
    schema: SchemaRef,
    total_rows: Option<u64>,
    batches: Receiver<Result<RecordBatch, FileError>>,
    finished: bool,
}

impl Loader {
    /// Starts reading `reader` on a new thread.
    ///
    /// # Errors
    /// [`FileError::Io`] if the thread cannot be started.
    pub fn spawn(reader: FileReader, wake: impl Fn() + Send + 'static) -> Result<Self, FileError> {
        let schema = reader.schema();
        let total_rows = reader.total_rows();
        let (sender, batches) = sync_channel(READ_AHEAD);
        thread::Builder::new()
            .name("densa-files loader".to_owned())
            .spawn(move || {
                let read = catch_unwind(AssertUnwindSafe(|| read_all(reader, &sender, &wake)));
                if read.is_err() {
                    let panicked = FileError::Invalid("reading the file panicked".to_owned());
                    if sender.send(Err(panicked)).is_err() {
                        return;
                    }
                }
                drop(sender);
                // A panic here is the end of the thread either way.
                let _ = catch_unwind(AssertUnwindSafe(&wake));
            })?;
        Ok(Self {
            schema,
            total_rows,
            batches,
            finished: false,
        })
    }

    /// The columns of every batch.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// How many rows the file holds, if its format says (Parquet).
    pub fn total_rows(&self) -> Option<u64> {
        self.total_rows
    }

    /// The next batch read, if one is ready: never waits. After an error, nothing more
    /// comes.
    pub fn try_next(&mut self) -> Option<Result<RecordBatch, FileError>> {
        match self.batches.try_recv() {
            Ok(batch) => Some(batch),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => {
                self.finished = true;
                None
            }
        }
    }

    /// The next batch read, waiting for it; `None` once the file is read.
    pub fn wait_next(&mut self) -> Option<Result<RecordBatch, FileError>> {
        let batch = self.batches.recv().ok();
        self.finished |= batch.is_none();
        batch
    }

    /// Whether every batch has been taken and the reading is over (seen by
    /// [`try_next`](Self::try_next) or [`wait_next`](Self::wait_next) finding no more).
    pub fn is_finished(&self) -> bool {
        self.finished
    }
}

/// Sends `reader`'s batches, waking the taker after each, until the end, an error, or the
/// taker is gone.
fn read_all(
    reader: FileReader,
    sender: &SyncSender<Result<RecordBatch, FileError>>,
    wake: &impl Fn(),
) {
    for batch in reader {
        let failed = batch.is_err();
        if sender.send(batch).is_err() {
            // The loader was dropped: nobody wants the rest.
            return;
        }
        wake();
        if failed {
            return;
        }
    }
}
