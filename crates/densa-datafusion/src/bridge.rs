//! Moves record batches between DataFusion's Arrow (59) and densa's (60) through Arrow IPC
//! streams: the format both versions read and write, whatever their Rust types. A batch is
//! written by one version and read by the other: two copies.
//!
//! Each crossing is a whole stream (schema, dictionaries, batch, end), read with a stream
//! reader: an incremental decoder holds back a message with an empty body (every schema,
//! and batches of only null-typed columns) until more bytes arrive. Re-encoding the schema
//! per batch costs little next to copying the batch.

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_schema::SchemaRef;
use datafusion::arrow::array::{Array, ArrayRef as DfArrayRef, AsArray, RecordBatch as DfBatch};
use datafusion::arrow::datatypes::{DataType as DfDataType, SchemaRef as DfSchemaRef};
use datafusion::arrow::ipc::reader::StreamReader as DfReader;
use datafusion::arrow::ipc::writer::StreamWriter as DfWriter;

/// View arrays holding more than this many bytes they do not use are compacted before
/// crossing: IPC writes a view array's buffers whole, and a filtered batch may point into
/// buffers far larger than its own rows.
const SPARE_VIEW_BYTES: usize = 64 << 10;

/// `schema` as densa's Arrow has it.
pub(crate) fn schema_in(schema: &DfSchemaRef) -> Result<SchemaRef, String> {
    let bytes = stream_out(schema, None)?;
    let reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(|e| e.to_string())?;
    Ok(reader.schema())
}

/// `batch` as densa's Arrow has it.
pub(crate) fn batch_in(batch: &DfBatch) -> Result<RecordBatch, String> {
    let batch = compact_views(batch)?;
    let bytes = stream_out(&batch.schema(), Some(&batch))?;
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(|e| e.to_string())?;
    match reader.next() {
        Some(batch) => batch.map_err(|e| e.to_string()),
        None => Err("a batch went missing on its way from DataFusion".to_owned()),
    }
}

/// `batch` with its string and binary views compacted where they hold mostly bytes of
/// other rows.
fn compact_views(batch: &DfBatch) -> Result<DfBatch, String> {
    let mut changed = false;
    let columns: Vec<DfArrayRef> = batch
        .columns()
        .iter()
        .map(|column| {
            let compacted: Option<DfArrayRef> = match column.data_type() {
                DfDataType::Utf8View => {
                    let views = column.as_string_view();
                    let held: usize = views.data_buffers().iter().map(|b| b.len()).sum();
                    (held > views.total_buffer_bytes_used() + SPARE_VIEW_BYTES)
                        .then(|| Arc::new(views.gc()) as DfArrayRef)
                }
                DfDataType::BinaryView => {
                    let views = column.as_binary_view();
                    let held: usize = views.data_buffers().iter().map(|b| b.len()).sum();
                    (held > views.total_buffer_bytes_used() + SPARE_VIEW_BYTES)
                        .then(|| Arc::new(views.gc()) as DfArrayRef)
                }
                _ => None,
            };
            changed |= compacted.is_some();
            compacted.unwrap_or_else(|| column.clone())
        })
        .collect();
    if !changed {
        return Ok(batch.clone());
    }
    DfBatch::try_new(batch.schema(), columns).map_err(|e| e.to_string())
}

/// A whole IPC stream, written by DataFusion's Arrow, of `schema` and `batch`.
fn stream_out(schema: &DfSchemaRef, batch: Option<&DfBatch>) -> Result<Vec<u8>, String> {
    let mut writer = DfWriter::try_new(Vec::new(), schema).map_err(|e| e.to_string())?;
    if let Some(batch) = batch {
        writer.write(batch).map_err(|e| e.to_string())?;
    }
    writer.finish().map_err(|e| e.to_string())?;
    writer.into_inner().map_err(|e| e.to_string())
}

/// densa batches as DataFusion's Arrow has them (to register them as a table), one batch
/// at a time so no more than a batch is held twice.
pub(crate) fn batches_out(
    given: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<(DfSchemaRef, Vec<DfBatch>), String> {
    let (schema, _) = stream_in(given, None)?;
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        if batch.schema_ref().fields() != given.fields() {
            return Err("a batch whose columns differ from the table's".to_owned());
        }
        let (_, converted) = stream_in(given, Some(batch))?;
        out.extend(converted);
    }
    Ok((schema, out))
}

/// A whole IPC stream of `schema` and `batch`, written by densa's Arrow and read by
/// DataFusion's.
fn stream_in(
    schema: &SchemaRef,
    batch: Option<&RecordBatch>,
) -> Result<(DfSchemaRef, Vec<DfBatch>), String> {
    let mut bytes = Vec::new();
    let mut writer =
        arrow_ipc::writer::StreamWriter::try_new(&mut bytes, schema).map_err(|e| e.to_string())?;
    if let Some(batch) = batch {
        writer.write(batch).map_err(|e| e.to_string())?;
    }
    writer.finish().map_err(|e| e.to_string())?;
    drop(writer);
    let reader = DfReader::try_new(Cursor::new(bytes), None).map_err(|e| e.to_string())?;
    let schema = reader.schema();
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    Ok((schema, batches))
}
