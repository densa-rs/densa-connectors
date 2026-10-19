use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use arrow_json::reader::Decoder as JsonDecoder;
use arrow_json::ReaderBuilder;
use arrow_schema::{ArrowError, SchemaRef};
use arrow_select::concat::concat_batches;

use crate::{MessageFormat, StreamError};

/// Turns messages into record batches, in the stream's format.
pub(crate) enum Decoder {
    Json(Box<Json>),
    Ipc(Box<Ipc>),
}

/// JSON rows: each message decoded on its own (so a bad one loses only itself), its rows
/// gathered with the earlier messages' into batches of `rows`.
pub(crate) struct Json {
    schema: SchemaRef,
    rows: usize,
    /// Empty between messages.
    scratch: JsonDecoder,
    /// Complete rows of earlier messages, not yet handed over.
    gathered: Vec<RecordBatch>,
    gathered_rows: usize,
}

/// An Arrow IPC stream, fed message by message.
pub(crate) struct Ipc {
    decoder: StreamDecoder,
    /// Bytes fed since the decoder last finished something: a cap on what a stream that
    /// declares a huge body can make it hold.
    unfinished: usize,
    max_unfinished: usize,
    had_schema: bool,
}

/// What went wrong with a message.
pub(crate) enum Problem {
    /// The message was skipped; the stream goes on.
    Skipped { reason: String, rows_lost: usize },
    /// The stream cannot be read further on this connection (an IPC stream out of step):
    /// start it over.
    Broken(String),
}

impl Decoder {
    pub(crate) fn new(
        format: &MessageFormat,
        rows: usize,
        max_message: usize,
    ) -> Result<Self, StreamError> {
        Ok(match format {
            MessageFormat::Json { schema } => Self::Json(Box::new(Json {
                schema: schema.clone(),
                rows,
                scratch: json_decoder(schema, rows)?,
                gathered: Vec::new(),
                gathered_rows: 0,
            })),
            MessageFormat::ArrowIpc => Self::Ipc(Box::new(Ipc {
                decoder: StreamDecoder::new(),
                unfinished: 0,
                max_unfinished: max_message.saturating_mul(4),
                had_schema: false,
            })),
        })
    }

    /// Starts over, as for a new connection (an IPC stream begins with its schema again).
    /// Rows not yet handed over are dropped: call [`flush`](Self::flush) first.
    pub(crate) fn reset(&mut self) {
        match self {
            Self::Json(json) => {
                json.scratch_fresh();
                json.gathered.clear();
                json.gathered_rows = 0;
            }
            Self::Ipc(ipc) => {
                ipc.decoder = StreamDecoder::new();
                ipc.unfinished = 0;
                ipc.had_schema = false;
            }
        }
    }

    /// Rows read but not yet in a batch.
    pub(crate) fn pending(&self) -> usize {
        match self {
            Self::Json(json) => json.gathered_rows,
            Self::Ipc(_) => 0,
        }
    }

    /// Reads a text message, pushing every full batch onto `out`.
    pub(crate) fn text(&mut self, bytes: &[u8], out: &mut Vec<RecordBatch>) -> Result<(), Problem> {
        match self {
            Self::Json(json) => json.message(bytes, out),
            Self::Ipc(_) => Err(Problem::Skipped {
                reason: "a text message in an Arrow IPC stream".to_owned(),
                rows_lost: 0,
            }),
        }
    }

    /// Reads a binary message, pushing every batch it completes onto `out`.
    pub(crate) fn binary(
        &mut self,
        bytes: &[u8],
        out: &mut Vec<RecordBatch>,
    ) -> Result<(), Problem> {
        match self {
            Self::Ipc(ipc) => ipc.message(bytes, out),
            Self::Json(_) => Err(Problem::Skipped {
                reason: "a binary message in a JSON stream".to_owned(),
                rows_lost: 0,
            }),
        }
    }

    /// The rows gathered so far as a batch, if there are any.
    pub(crate) fn flush(&mut self) -> Result<Option<RecordBatch>, Problem> {
        match self {
            Self::Json(json) => json.take_all(),
            Self::Ipc(_) => Ok(None),
        }
    }
}

impl Json {
    fn scratch_fresh(&mut self) {
        // The schema was accepted when the stream started.
        if let Ok(fresh) = json_decoder(&self.schema, self.rows) {
            self.scratch = fresh;
        }
    }

    /// Decodes one message whole: its rows join the gathered ones only if all of it reads.
    fn message(&mut self, bytes: &[u8], out: &mut Vec<RecordBatch>) -> Result<(), Problem> {
        let mut decoded = Vec::new();
        let result = decode_all(&mut self.scratch, bytes, self.rows, &mut decoded);
        if let Err(error) = result {
            // Whole rows only: the one being read when it failed was never a row.
            let buffered = self.scratch.len() - usize::from(self.scratch.has_partial_record());
            let rows_lost = decoded.iter().map(RecordBatch::num_rows).sum::<usize>() + buffered;
            self.scratch_fresh();
            return Err(Problem::Skipped {
                reason: error.to_string(),
                rows_lost,
            });
        }
        for batch in decoded {
            self.gathered_rows += batch.num_rows();
            self.gathered.push(batch);
        }
        if self.gathered_rows >= self.rows {
            self.hand_over_full(out)?;
        }
        Ok(())
    }

    /// Hands over every full batch of the gathered rows, keeping the rest.
    fn hand_over_full(&mut self, out: &mut Vec<RecordBatch>) -> Result<(), Problem> {
        let Some(all) = self.take_all()? else {
            return Ok(());
        };
        let mut at = 0;
        while all.num_rows() - at >= self.rows {
            out.push(all.slice(at, self.rows));
            at += self.rows;
        }
        if at < all.num_rows() {
            let rest = all.slice(at, all.num_rows() - at);
            self.gathered_rows = rest.num_rows();
            self.gathered.push(rest);
        }
        Ok(())
    }

    /// Every gathered row as one batch.
    fn take_all(&mut self) -> Result<Option<RecordBatch>, Problem> {
        if self.gathered.is_empty() {
            return Ok(None);
        }
        let rows = self.gathered_rows;
        let batches = std::mem::take(&mut self.gathered);
        self.gathered_rows = 0;
        if batches.len() == 1 {
            return Ok(batches.into_iter().next());
        }
        concat_batches(&self.schema, &batches)
            .map(Some)
            .map_err(|error| Problem::Skipped {
                reason: error.to_string(),
                rows_lost: rows,
            })
    }
}

/// Decodes all of `bytes` with `decoder` (empty on entry, and on return when it succeeds),
/// pushing its rows onto `out` in batches of at most `rows`. A record cut off at the end
/// of the message is an error.
fn decode_all(
    decoder: &mut JsonDecoder,
    bytes: &[u8],
    rows: usize,
    out: &mut Vec<RecordBatch>,
) -> Result<(), ArrowError> {
    let mut rest = bytes;
    while !rest.is_empty() {
        let read = decoder.decode(rest)?;
        rest = &rest[read..];
        if decoder.len() >= rows {
            out.extend(decoder.flush()?);
        } else if read == 0 {
            break;
        }
    }
    out.extend(decoder.flush()?.filter(|batch| batch.num_rows() > 0));
    Ok(())
}

impl Ipc {
    fn message(&mut self, bytes: &[u8], out: &mut Vec<RecordBatch>) -> Result<(), Problem> {
        self.unfinished = self.unfinished.saturating_add(bytes.len());
        let mut buffer = Buffer::from_vec(bytes.to_vec());
        while !buffer.is_empty() {
            match self.decoder.decode(&mut buffer) {
                Ok(Some(batch)) => {
                    self.unfinished = 0;
                    if batch.num_rows() > 0 {
                        out.push(batch);
                    }
                }
                Ok(None) => {
                    if !self.had_schema && self.decoder.schema().is_some() {
                        self.had_schema = true;
                        self.unfinished = 0;
                    }
                    if buffer.is_empty() {
                        break;
                    }
                }
                Err(error) => {
                    return Err(Problem::Broken(format!("unreadable Arrow IPC: {error}")))
                }
            }
        }
        if self.unfinished > self.max_unfinished {
            return Err(Problem::Broken(format!(
                "an Arrow IPC message over {} bytes",
                self.max_unfinished
            )));
        }
        Ok(())
    }
}

fn json_decoder(schema: &SchemaRef, rows: usize) -> Result<JsonDecoder, StreamError> {
    ReaderBuilder::new(schema.clone())
        .with_batch_size(rows)
        .build_decoder()
        .map_err(|error| StreamError::Schema(error.to_string()))
}
