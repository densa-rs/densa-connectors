use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use arrow_json::reader::{infer_json_schema_from_iterator, Decoder as JsonDecoder};
use arrow_json::ReaderBuilder;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use arrow_select::concat::concat_batches;
use serde_json::Value;

use crate::{MessageFormat, StreamError};

/// Turns messages into record batches, in the stream's format.
pub(crate) enum Decoder {
    Json(Box<Json>),
    Ipc(Box<Ipc>),
}

/// JSON rows: each message decoded on its own (so a bad one loses only itself), its rows
/// gathered with the earlier messages' into batches of `rows`.
pub(crate) struct Json {
    /// `None` until the first message with objects gives it, for a stream whose columns
    /// are inferred.
    schema: Option<SchemaRef>,
    /// Whether the columns were inferred: numbers and booleans are then taken as text in a
    /// text column.
    inferred: bool,
    rows: usize,
    /// Empty between messages; `None` while there is no schema.
    scratch: Option<JsonDecoder>,
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
                schema: Some(schema.clone()),
                inferred: false,
                rows,
                scratch: Some(json_decoder(schema, rows, false)?),
                gathered: Vec::new(),
                gathered_rows: 0,
            })),
            MessageFormat::JsonInferred => Self::Json(Box::new(Json {
                schema: None,
                inferred: true,
                rows,
                scratch: None,
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
        // The schema was accepted when the stream started, or when it was inferred.
        let fresh = self
            .schema
            .as_ref()
            .map(|schema| json_decoder(schema, self.rows, self.inferred));
        if let Some(Ok(fresh)) = fresh {
            self.scratch = Some(fresh);
        }
    }

    /// Decodes one message whole: its rows join the gathered ones only if all of it reads.
    /// Without a schema yet, the message's objects give it first.
    fn message(&mut self, bytes: &[u8], out: &mut Vec<RecordBatch>) -> Result<(), Problem> {
        if self.scratch.is_none() {
            let Some(schema) = infer(bytes)? else {
                // Nothing to read the columns from: the next message may have them.
                return Ok(());
            };
            let decoder =
                json_decoder(&schema, self.rows, true).map_err(|error| Problem::Skipped {
                    reason: error.to_string(),
                    rows_lost: 0,
                })?;
            self.schema = Some(schema);
            self.scratch = Some(decoder);
        }
        let Some(scratch) = self.scratch.as_mut() else {
            return Ok(());
        };
        let mut decoded = Vec::new();
        let result = decode_all(scratch, bytes, self.rows, &mut decoded);
        if let Err(error) = result {
            // Whole rows only: the one being read when it failed was never a row.
            let buffered = scratch.len() - usize::from(scratch.has_partial_record());
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
        concat_batches(&batches[0].schema(), &batches)
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

fn json_decoder(
    schema: &SchemaRef,
    rows: usize,
    text_takes_primitives: bool,
) -> Result<JsonDecoder, StreamError> {
    ReaderBuilder::new(schema.clone())
        .with_batch_size(rows)
        .with_coerce_primitive(text_takes_primitives)
        .build_decoder()
        .map_err(|error| StreamError::Schema(error.to_string()))
}

/// The columns of the JSON objects in `bytes`, typed by their values there, with numbers as
/// `Float64` and a field that is null in all of them as text; `None` if the message has no
/// fields to read.
fn infer(bytes: &[u8]) -> Result<Option<SchemaRef>, Problem> {
    let values = serde_json::Deserializer::from_slice(bytes)
        .into_iter::<Value>()
        .map(|value| value.map_err(|error| ArrowError::JsonError(error.to_string())));
    let schema = infer_json_schema_from_iterator(values).map_err(|error| Problem::Skipped {
        reason: format!("no columns could be read from it: {error}"),
        rows_lost: 0,
    })?;
    if schema.fields().is_empty() {
        return Ok(None);
    }
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Null => Field::new(field.name(), DataType::Utf8, true),
            DataType::Int64 => Field::new(field.name(), DataType::Float64, true),
            _ => field.as_ref().clone(),
        })
        .collect();
    Ok(Some(Arc::new(Schema::new(fields))))
}

#[cfg(test)]
mod tests {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use arrow_array::Array;

    use super::*;

    fn inferring() -> Decoder {
        Decoder::new(&MessageFormat::JsonInferred, 100, 1 << 20).unwrap()
    }

    /// Reads `messages` as text, then everything gathered, as one batch.
    fn read(decoder: &mut Decoder, messages: &[&str]) -> Vec<Result<(), String>> {
        let mut out = Vec::new();
        messages
            .iter()
            .map(|message| {
                decoder
                    .text(message.as_bytes(), &mut out)
                    .map_err(|problem| match problem {
                        Problem::Skipped { reason, .. } | Problem::Broken(reason) => reason,
                    })
            })
            .collect()
    }

    fn gathered(decoder: &mut Decoder) -> RecordBatch {
        decoder.flush().ok().flatten().expect("rows gathered")
    }

    fn columns(batch: &RecordBatch) -> Vec<(String, DataType)> {
        let schema = batch.schema();
        let fields = schema.fields().iter();
        fields
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect()
    }

    #[test]
    fn the_first_message_gives_the_columns_in_the_order_they_come() {
        let mut decoder = inferring();
        let first =
            r#"{"sensor":"a","celsius":21,"ok":true} {"sensor":"b","celsius":21.5,"note":null}"#;
        assert!(read(&mut decoder, &[first]).iter().all(Result::is_ok));
        let batch = gathered(&mut decoder);
        assert_eq!(
            columns(&batch),
            [
                ("sensor".to_owned(), DataType::Utf8),
                ("celsius".to_owned(), DataType::Float64),
                ("ok".to_owned(), DataType::Boolean),
                ("note".to_owned(), DataType::Utf8),
            ],
            "numbers are floats; a field always null is text"
        );
        assert_eq!(batch.num_rows(), 2);
        let celsius = batch.column(1).as_primitive::<Float64Type>();
        assert_eq!(celsius.values(), &[21.0, 21.5]);
    }

    #[test]
    fn later_messages_keep_the_columns() {
        let mut decoder = inferring();
        let results = read(
            &mut decoder,
            &[
                r#"{"id":1,"name":"a","note":null}"#,
                r#"{"id":2,"name":7,"note":true,"extra":"ignored"}"#,
                r#"{"id":2.5,"name":"c"}"#,
            ],
        );
        assert!(results.iter().all(Result::is_ok), "{results:?}");
        let batch = gathered(&mut decoder);
        assert_eq!(
            batch.num_columns(),
            3,
            "a field that comes later is not a column"
        );
        assert_eq!(
            batch.column(0).as_primitive::<Float64Type>().values(),
            &[1.0, 2.0, 2.5],
            "fractions after whole numbers are kept"
        );
        let names = batch.column(1).as_string::<i32>();
        assert_eq!(
            names.value(1),
            "7",
            "a number in a text column is kept as text"
        );
        let notes = batch.column(2).as_string::<i32>();
        assert!(notes.is_null(0));
        assert_eq!(notes.value(1), "true");
    }

    #[test]
    fn messages_without_fields_wait_for_one_with_them() {
        let mut decoder = inferring();
        let results = read(&mut decoder, &["", "  \n", "{}", r#"{"x":1}"#]);
        assert!(results.iter().all(Result::is_ok), "{results:?}");
        assert_eq!(
            columns(&gathered(&mut decoder)),
            [("x".to_owned(), DataType::Float64)]
        );
    }

    #[test]
    fn a_message_that_is_not_objects_is_skipped_and_the_next_one_still_counts() {
        let mut decoder = inferring();
        let results = read(&mut decoder, &["[1, 2]", "not json", r#"{"x":"y"}"#]);
        assert!(results[0].is_err() && results[1].is_err(), "{results:?}");
        assert!(results[0]
            .as_ref()
            .unwrap_err()
            .contains("no columns could be read"));
        assert!(results[2].is_ok());
        assert_eq!(
            columns(&gathered(&mut decoder)),
            [("x".to_owned(), DataType::Utf8)]
        );
    }

    #[test]
    fn the_columns_outlive_a_new_connection() {
        let mut decoder = inferring();
        read(&mut decoder, &[r#"{"x":1}"#]);
        decoder.reset();
        let results = read(&mut decoder, &[r#"{"x":2,"y":"new"}"#]);
        assert!(results[0].is_ok());
        assert_eq!(
            columns(&gathered(&mut decoder)),
            [("x".to_owned(), DataType::Float64)]
        );
    }

    #[test]
    fn values_that_do_not_fit_skip_their_message() {
        let mut decoder = inferring();
        let results = read(
            &mut decoder,
            &[r#"{"x":1}"#, r#"{"x":"text"}"#, r#"{"x":3}"#],
        );
        assert!(results[1].is_err(), "text in a number column: {results:?}");
        let batch = gathered(&mut decoder);
        assert_eq!(
            batch.column(0).as_primitive::<Float64Type>().values(),
            &[1.0, 3.0]
        );
    }

    #[test]
    fn a_given_schema_does_not_take_numbers_as_text() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let format = MessageFormat::Json { schema };
        let mut decoder = Decoder::new(&format, 100, 1 << 20).unwrap();
        let results = read(&mut decoder, &[r#"{"name":7}"#]);
        assert!(
            results[0].is_err(),
            "as before: a given schema is read strictly"
        );
    }
}
