use std::fmt;
use std::fs::File;
use std::io::{BufReader, Seek};
use std::path::Path;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchReader, UInt64Array};
use arrow_csv::reader::Format;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

use crate::error::{invalid, FileError};

/// What a file holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FileFormat {
    /// Comma- (or otherwise) separated values, one record per line.
    Csv,
    /// Apache Parquet.
    Parquet,
}

impl FileFormat {
    /// The format a path's extension names (`csv`, `tsv`, `parquet`, `parq`, `pq`, in any
    /// case), or `None`.
    pub fn from_path(path: &Path) -> Option<Self> {
        let extension = path.extension()?.to_str()?.to_ascii_lowercase();
        match extension.as_str() {
            "csv" | "tsv" => Some(Self::Csv),
            "parquet" | "parq" | "pq" => Some(Self::Parquet),
            _ => None,
        }
    }
}

/// How to open a file: [`OpenOptions::new`], then the settings that differ, then
/// [`open`](Self::open).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenOptions {
    format: Option<FileFormat>,
    batch_rows: usize,
    delimiter: Option<u8>,
    header: bool,
    infer_rows: usize,
    text: bool,
    row_numbers: Option<String>,
}

/// The most rows a batch holds: readers set aside room for a whole batch before reading it.
pub const MAX_BATCH_ROWS: usize = 1 << 20;

impl OpenOptions {
    /// The defaults: the format from the extension, batches of 8,192 rows, CSV with a header
    /// line, its column types inferred from the first 1,000 records, no row numbers.
    pub fn new() -> Self {
        Self {
            format: None,
            batch_rows: 8_192,
            delimiter: None,
            header: true,
            infer_rows: 1_000,
            text: false,
            row_numbers: None,
        }
    }

    /// Reads the file as `format`, whatever its extension.
    #[must_use]
    pub fn format(mut self, format: FileFormat) -> Self {
        self.format = Some(format);
        self
    }

    /// Batches of at most `rows` rows (from one to [`MAX_BATCH_ROWS`]).
    #[must_use]
    pub fn batch_rows(mut self, rows: usize) -> Self {
        self.batch_rows = rows.clamp(1, MAX_BATCH_ROWS);
        self
    }

    /// CSV fields separated by `delimiter` (by default a tab for `.tsv`, else a comma).
    #[must_use]
    pub fn delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    /// Whether a CSV file starts with a line of column names (by default it does; without
    /// one, columns are named `column_1`, `column_2`, ...).
    #[must_use]
    pub fn header(mut self, header: bool) -> Self {
        self.header = header;
        self
    }

    /// Infers CSV column types from the first `records` records (at least one; `usize::MAX`
    /// reads the whole file first). A later value that does not fit its column's type is an
    /// error that ends the reading: infer from more records, or read every column as text
    /// ([`as_text`](Self::as_text)). A column empty in every record inferred from is text.
    #[must_use]
    pub fn infer_rows(mut self, records: usize) -> Self {
        self.infer_rows = records.max(1);
        self
    }

    /// Reads every CSV column as text, whatever its values look like: a file whose types
    /// change partway through still reads to its end.
    #[must_use]
    pub fn as_text(mut self) -> Self {
        self.text = true;
        self
    }

    /// Adds a column of row numbers named `name` (an unsigned integer from 0, never null):
    /// a key for a table when the file has none.
    #[must_use]
    pub fn row_numbers(mut self, name: impl Into<String>) -> Self {
        self.row_numbers = Some(name.into());
        self
    }

    /// Opens the file at `path`: reads its schema (for CSV, infers it), then reads batches
    /// as they are asked for.
    ///
    /// # Errors
    /// [`FileError::UnknownFormat`] if no format was given and the extension names none,
    /// [`FileError::Io`] if the file cannot be read, [`FileError::Invalid`] if it is not
    /// valid in its format, [`FileError::DuplicateColumn`] if two columns have one name (or
    /// the row number column's name is taken).
    pub fn open(&self, path: impl AsRef<Path>) -> Result<FileReader, FileError> {
        let path = path.as_ref();
        let format = self
            .format
            .or_else(|| FileFormat::from_path(path))
            .ok_or_else(|| FileError::UnknownFormat(path.to_owned()))?;
        let file = File::open(path)?;
        let (inner, schema, total_rows) = match format {
            FileFormat::Csv => self.open_csv(file, path)?,
            FileFormat::Parquet => self.open_parquet(file)?,
        };
        if let Some(name) = first_duplicate(&schema) {
            return Err(FileError::DuplicateColumn(name.to_owned()));
        }
        let numbers = match &self.row_numbers {
            Some(name) => Some(RowNumbers::new(name, &schema)?),
            None => None,
        };
        let schema = match &numbers {
            Some(numbers) => numbers.schema.clone(),
            None => schema,
        };
        Ok(FileReader {
            format,
            schema,
            total_rows,
            inner,
            numbers,
        })
    }

    fn open_csv(
        &self,
        mut file: File,
        path: &Path,
    ) -> Result<(Inner, SchemaRef, Option<u64>), FileError> {
        let tsv = path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("tsv"));
        let delimiter = self.delimiter.unwrap_or(if tsv { b'\t' } else { b',' });
        let format = Format::default()
            .with_header(self.header)
            .with_delimiter(delimiter);
        let (schema, _) = format
            .infer_schema(BufReader::new(&mut file), Some(self.infer_rows))
            .map_err(invalid)?;
        file.rewind()?;
        // Text where nothing told the type (a column empty in every record looked at), or
        // everywhere if asked.
        let fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                DataType::Null => field.as_ref().clone().with_data_type(DataType::Utf8),
                _ if self.text => field.as_ref().clone().with_data_type(DataType::Utf8),
                _ => field.as_ref().clone(),
            })
            .collect();
        let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        let reader = arrow_csv::ReaderBuilder::new(schema.clone())
            .with_format(format)
            .with_batch_size(self.batch_rows)
            .build(BufReader::new(file))
            .map_err(invalid)?;
        Ok((Inner::Csv(Box::new(reader)), schema, None))
    }

    fn open_parquet(&self, file: File) -> Result<(Inner, SchemaRef, Option<u64>), FileError> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(invalid)?;
        let total = u64::try_from(builder.metadata().file_metadata().num_rows()).ok();
        let reader = builder
            .with_batch_size(self.batch_rows)
            .build()
            .map_err(invalid)?;
        let schema = reader.schema();
        Ok((Inner::Parquet(reader), schema, total))
    }
}

/// The first name two columns of `schema` share.
fn first_duplicate(schema: &Schema) -> Option<&str> {
    let fields = schema.fields();
    fields
        .iter()
        .enumerate()
        .find(|(i, field)| fields[..*i].iter().any(|f| f.name() == field.name()))
        .map(|(_, field)| field.name().as_str())
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Opens the file at `path` with the default [`OpenOptions`]: its format from the
/// extension.
///
/// # Errors
/// As [`OpenOptions::open`].
pub fn open(path: impl AsRef<Path>) -> Result<FileReader, FileError> {
    OpenOptions::new().open(path)
}

/// The reader of one format.
enum Inner {
    /// Boxed: a CSV reader is several times larger than a Parquet one.
    Csv(Box<arrow_csv::Reader<BufReader<File>>>),
    Parquet(ParquetRecordBatchReader),
}

/// The row number column being added: its schema, and the next number.
#[derive(Debug)]
struct RowNumbers {
    schema: SchemaRef,
    next: u64,
}

impl RowNumbers {
    fn new(name: &str, schema: &Schema) -> Result<Self, FileError> {
        if schema.column_with_name(name).is_some() {
            return Err(FileError::DuplicateColumn(name.to_owned()));
        }
        let mut fields: Vec<_> = schema.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(name, DataType::UInt64, false)));
        Ok(Self {
            schema: Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
            next: 0,
        })
    }

    /// `batch` with its rows' numbers after its columns.
    fn add(&mut self, batch: &RecordBatch) -> Result<RecordBatch, FileError> {
        // Lossless: a batch holds fewer rows than u64 can count.
        let rows = batch.num_rows() as u64;
        let numbers = UInt64Array::from_iter_values(self.next..self.next + rows);
        self.next += rows;
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns.push(Arc::new(numbers));
        RecordBatch::try_new(self.schema.clone(), columns).map_err(invalid)
    }
}

/// A file being read: its schema now, its rows as record batches when asked for (an
/// iterator; empty batches are skipped).
pub struct FileReader {
    format: FileFormat,
    schema: SchemaRef,
    total_rows: Option<u64>,
    inner: Inner,
    numbers: Option<RowNumbers>,
}

impl FileReader {
    /// The columns of every batch (with the row number column last, if asked for).
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// The file's format.
    pub fn format(&self) -> FileFormat {
        self.format
    }

    /// How many rows the file holds, if its format says before reading them (Parquet does,
    /// CSV does not).
    pub fn total_rows(&self) -> Option<u64> {
        self.total_rows
    }
}

impl Iterator for FileReader {
    type Item = Result<RecordBatch, FileError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let batch = match &mut self.inner {
                Inner::Csv(reader) => reader.next()?,
                Inner::Parquet(reader) => reader.next()?,
            };
            let batch = match batch {
                Ok(batch) if batch.num_rows() == 0 => continue,
                Ok(batch) => batch,
                Err(error) => return Some(Err(invalid(error))),
            };
            return Some(match &mut self.numbers {
                Some(numbers) => numbers.add(&batch),
                None => Ok(batch),
            });
        }
    }
}

impl fmt::Debug for FileReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileReader")
            .field("format", &self.format)
            .field("schema", &self.schema)
            .field("total_rows", &self.total_rows)
            .finish_non_exhaustive()
    }
}
