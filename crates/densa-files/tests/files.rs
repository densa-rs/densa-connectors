//! Reading CSV and Parquet files as record batches, directly and on a loader thread.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type, UInt64Type};
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use densa_files::{open, FileError, FileFormat, Loader, OpenOptions};
use parquet::arrow::ArrowWriter;

/// A file in the temporary folder, deleted when dropped.
struct TempFile(PathBuf);

impl TempFile {
    fn new(name: &str, contents: &[u8]) -> Self {
        let path = std::env::temp_dir().join(format!("densa-files-{}-{name}", std::process::id()));
        fs::write(&path, contents).unwrap();
        Self(path)
    }

    /// A Parquet file of `batches`.
    fn parquet(name: &str, batches: &[RecordBatch]) -> Self {
        let file = Self::new(name, b"");
        let mut writer = ArrowWriter::try_new(
            fs::File::create(&file.0).unwrap(),
            batches[0].schema(),
            None,
        )
        .unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();
        file
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn int(batch: &RecordBatch, column: usize) -> Vec<i64> {
    batch
        .column(column)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec()
}

const ORDERS: &str = "symbol,qty,price\nA,1,1.5\nB,2,2.25\nC,3,3.0\nD,4,4.75\nE,5,5.5\n";

#[test]
fn a_csv_files_types_are_inferred_from_its_records() {
    let file = TempFile::new("orders.csv", ORDERS.as_bytes());
    let reader = open(&file.0).unwrap();
    assert_eq!(reader.format(), FileFormat::Csv);
    assert_eq!(reader.total_rows(), None);
    let schema = reader.schema();
    let types: Vec<(&str, &DataType)> = schema
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.data_type()))
        .collect();
    assert_eq!(
        types,
        [
            ("symbol", &DataType::Utf8),
            ("qty", &DataType::Int64),
            ("price", &DataType::Float64)
        ]
    );
    let batches: Vec<RecordBatch> = reader.map(Result::unwrap).collect();
    assert_eq!(batches.len(), 1);
    assert_eq!(int(&batches[0], 1), [1, 2, 3, 4, 5]);
    let prices = batches[0]
        .column(2)
        .as_primitive::<Float64Type>()
        .values()
        .to_vec();
    assert_eq!(prices, [1.5, 2.25, 3.0, 4.75, 5.5]);
}

#[test]
fn rows_come_in_batches_of_the_size_asked_for() {
    let file = TempFile::new("batches.csv", ORDERS.as_bytes());
    let sizes: Vec<usize> = OpenOptions::new()
        .batch_rows(2)
        .open(&file.0)
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .collect();
    assert_eq!(sizes, [2, 2, 1]);
    let zero = OpenOptions::new().batch_rows(0).open(&file.0).unwrap();
    assert_eq!(zero.count(), 5, "at least one row a batch");
}

#[test]
fn tabs_semicolons_and_files_without_a_header_are_read() {
    let tsv = TempFile::new("tabs.TSV", b"a\tb\n1\tx\n2\ty\n");
    let batch = open(&tsv.0).unwrap().next().unwrap().unwrap();
    assert_eq!(int(&batch, 0), [1, 2]);

    let semi = TempFile::new("semi.csv", b"a;b\n1;x\n");
    let batch = OpenOptions::new()
        .delimiter(b';')
        .open(&semi.0)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(batch.num_columns(), 2);

    let bare = TempFile::new("bare.csv", b"1,x\n2,y\n");
    let reader = OpenOptions::new().header(false).open(&bare.0).unwrap();
    assert_eq!(reader.schema().field(0).name(), "column_1");
    assert_eq!(int(&reader.into_iter().next().unwrap().unwrap(), 0), [1, 2]);
}

#[test]
fn a_value_past_the_records_types_were_inferred_from_is_an_error() {
    let mut text = String::from("n\n");
    for i in 0..10 {
        text.push_str(&format!("{i}\n"));
    }
    text.push_str("not a number\n");
    let file = TempFile::new("late.csv", text.as_bytes());
    let results: Vec<_> = OpenOptions::new()
        .infer_rows(5)
        .open(&file.0)
        .unwrap()
        .collect();
    assert!(
        matches!(results.last(), Some(Err(FileError::Invalid(_)))),
        "{results:?}"
    );
    // Inferring from every record makes the column text instead.
    let reader = OpenOptions::new().infer_rows(100).open(&file.0).unwrap();
    assert_eq!(reader.schema().field(0).data_type(), &DataType::Utf8);
}

fn orders(ids: std::ops::Range<i64>) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(ids.clone())) as ArrayRef,
        ),
        (
            "name",
            Arc::new(StringArray::from_iter_values(
                ids.clone().map(|i| format!("n{i}")),
            )),
        ),
        (
            "price",
            Arc::new(Float64Array::from_iter_values(ids.map(|i| i as f64 / 2.0))),
        ),
    ])
    .unwrap()
}

#[test]
fn a_parquet_file_reads_back_as_written_and_tells_its_row_count() {
    let file = TempFile::parquet("orders.parquet", &[orders(0..700), orders(700..1_000)]);
    let reader = OpenOptions::new().batch_rows(256).open(&file.0).unwrap();
    assert_eq!(reader.format(), FileFormat::Parquet);
    assert_eq!(reader.total_rows(), Some(1_000));
    assert_eq!(reader.schema().fields().len(), 3);
    let batches: Vec<RecordBatch> = reader.map(Result::unwrap).collect();
    assert!(batches.iter().all(|b| b.num_rows() <= 256));
    let ids: Vec<i64> = batches.iter().flat_map(|b| int(b, 0)).collect();
    assert_eq!(ids, (0..1_000).collect::<Vec<_>>());
}

#[test]
fn row_numbers_count_on_across_batches() {
    let file = TempFile::parquet("numbered.parquet", &[orders(10..20)]);
    let reader = OpenOptions::new()
        .batch_rows(3)
        .row_numbers("row")
        .open(&file.0)
        .unwrap();
    let schema = reader.schema();
    let last = schema.field(schema.fields().len() - 1);
    assert_eq!(
        (last.name().as_str(), last.data_type(), last.is_nullable()),
        ("row", &DataType::UInt64, false)
    );
    let numbers: Vec<u64> = reader
        .map(Result::unwrap)
        .flat_map(|b| b.column(3).as_primitive::<UInt64Type>().values().to_vec())
        .collect();
    assert_eq!(numbers, (0..10).collect::<Vec<_>>());

    let taken = OpenOptions::new()
        .row_numbers("price")
        .open(&file.0)
        .unwrap_err();
    assert!(matches!(&taken, FileError::DuplicateColumn(name) if name == "price"));
    assert_eq!(
        taken.to_string(),
        "the file already has a column named \"price\""
    );
}

#[test]
fn formats_come_from_extensions_or_are_given() {
    assert_eq!(
        FileFormat::from_path("a.PARQUET".as_ref()),
        Some(FileFormat::Parquet)
    );
    assert_eq!(
        FileFormat::from_path("a.pq".as_ref()),
        Some(FileFormat::Parquet)
    );
    assert_eq!(
        FileFormat::from_path("a.parq".as_ref()),
        Some(FileFormat::Parquet)
    );
    assert_eq!(
        FileFormat::from_path("a.Csv".as_ref()),
        Some(FileFormat::Csv)
    );
    assert_eq!(FileFormat::from_path("a.txt".as_ref()), None);
    assert_eq!(FileFormat::from_path("noextension".as_ref()), None);

    let text = TempFile::new("orders.txt", ORDERS.as_bytes());
    let unknown = open(&text.0).unwrap_err();
    assert!(matches!(unknown, FileError::UnknownFormat(_)));
    assert!(unknown.to_string().starts_with("cannot tell the format of"));
    let reader = OpenOptions::new()
        .format(FileFormat::Csv)
        .open(&text.0)
        .unwrap();
    assert_eq!(reader.count(), 1);
}

#[test]
fn missing_and_broken_files_are_errors() {
    let missing = open(std::env::temp_dir().join("densa-files-no-such-file.csv")).unwrap_err();
    assert!(matches!(missing, FileError::Io(_)));
    assert!(std::error::Error::source(&missing).is_some());
    let broken = TempFile::new("broken.parquet", b"this is not parquet at all");
    let error = open(&broken.0).unwrap_err();
    assert!(matches!(error, FileError::Invalid(_)), "{error:?}");
    assert!(error.to_string().starts_with("invalid file:"));
    assert!(std::error::Error::source(&error).is_none());
    // A folder named like a file (as Spark writes them) says so.
    let folder =
        std::env::temp_dir().join(format!("densa-files-{}-folder.parquet", std::process::id()));
    fs::create_dir_all(&folder).unwrap();
    let error = open(&folder).unwrap_err();
    let _ = fs::remove_dir(&folder);
    assert!(matches!(error, FileError::Folder(_)), "{error:?}");
}

#[test]
fn a_loader_hands_over_every_batch_in_order_and_wakes_for_each() {
    let file = TempFile::parquet("loaded.parquet", &[orders(0..10_000)]);
    let reader = OpenOptions::new().batch_rows(1_000).open(&file.0).unwrap();
    let wakes = Arc::new(AtomicUsize::new(0));
    let counter = wakes.clone();
    let mut loader = Loader::spawn(reader, move || {
        counter.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    assert_eq!(loader.total_rows(), Some(10_000));
    assert_eq!(loader.schema().fields().len(), 3);
    let mut ids = Vec::new();
    while let Some(batch) = loader.wait_next() {
        ids.extend(int(&batch.unwrap(), 0));
    }
    assert_eq!(ids, (0..10_000).collect::<Vec<_>>());
    assert!(loader.is_finished());
    assert!(loader.try_next().is_none());
    // Ten batches, then the end. The last wake follows the end being visible (so a woken
    // interface sees it): give it a moment.
    let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while wakes.load(Ordering::Relaxed) < 11 && std::time::Instant::now() < until {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(wakes.load(Ordering::Relaxed), 11);
}

#[test]
fn a_loader_is_finished_only_once_the_end_is_seen() {
    let file = TempFile::new("small.csv", ORDERS.as_bytes());
    let mut loader = Loader::spawn(open(&file.0).unwrap(), || {}).unwrap();
    assert!(!loader.is_finished());
    let mut batches = 0;
    while !loader.is_finished() {
        if let Some(batch) = loader.try_next() {
            batch.unwrap();
            batches += 1;
        } else {
            std::thread::yield_now();
        }
    }
    assert_eq!(batches, 1);
}

#[test]
fn a_loader_stops_at_the_first_error() {
    let mut text = String::from("n\n");
    for i in 0..10 {
        text.push_str(&format!("{i}\n"));
    }
    text.push_str("oops\n11\n");
    let file = TempFile::new("stops.csv", text.as_bytes());
    let reader = OpenOptions::new()
        .infer_rows(5)
        .batch_rows(4)
        .open(&file.0)
        .unwrap();
    let mut loader = Loader::spawn(reader, || {}).unwrap();
    let results: Vec<_> = std::iter::from_fn(|| loader.wait_next()).collect();
    assert!(results.last().unwrap().is_err());
    assert_eq!(
        results.iter().filter(|r| r.is_err()).count(),
        1,
        "nothing after the error"
    );
}

#[test]
fn dropping_a_loader_stops_its_thread() {
    let file = TempFile::parquet("dropped.parquet", &[orders(0..50_000)]);
    let reader = OpenOptions::new().batch_rows(100).open(&file.0).unwrap();
    let wakes = Arc::new(AtomicUsize::new(0));
    let counter = wakes.clone();
    let mut loader = Loader::spawn(reader, move || {
        counter.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    loader.wait_next().unwrap().unwrap();
    drop(loader);
    std::thread::sleep(std::time::Duration::from_millis(200));
    let seen = wakes.load(Ordering::Relaxed);
    // A few batches read ahead at most, nowhere near the 500 there are.
    assert!(seen < 20, "{seen} wakes");
}

#[test]
fn a_huge_batch_size_is_held_to_the_largest() {
    let file = TempFile::new("huge.csv", ORDERS.as_bytes());
    let options = OpenOptions::new().batch_rows(usize::MAX);
    let rows: usize = options
        .open(&file.0)
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum();
    assert_eq!(rows, 5);
    assert_eq!(densa_files::MAX_BATCH_ROWS, 1 << 20);
}

#[test]
fn a_panic_while_loading_is_an_error_not_the_end() {
    let file = TempFile::parquet("panics.parquet", &[orders(0..100)]);
    let reader = OpenOptions::new().batch_rows(10).open(&file.0).unwrap();
    let mut loader = Loader::spawn(reader, || panic!("a waker that panics")).unwrap();
    let results: Vec<_> = std::iter::from_fn(|| loader.wait_next()).collect();
    assert_eq!(results.len(), 2, "the first batch, then the panic");
    assert!(results[0].is_ok());
    assert!(matches!(&results[1], Err(FileError::Invalid(m)) if m.contains("panicked")));
    assert!(loader.is_finished());
}

#[test]
fn empty_columns_read_as_text_and_every_column_can() {
    let file = TempFile::new("empty.csv", b"a,b,c\n1,,x\n2,,y\n");
    let schema = open(&file.0).unwrap().schema();
    let types: Vec<&DataType> = schema.fields().iter().map(|f| f.data_type()).collect();
    assert_eq!(types, [&DataType::Int64, &DataType::Utf8, &DataType::Utf8]);

    let reader = OpenOptions::new().as_text().open(&file.0).unwrap();
    assert!(reader
        .schema()
        .fields()
        .iter()
        .all(|f| f.data_type() == &DataType::Utf8));
    let batch = reader.into_iter().next().unwrap().unwrap();
    assert_eq!(batch.column(0).as_string::<i32>().value(1), "2");

    // A type that changes partway through reads to the end as text.
    let mut text = String::from("n\n");
    for i in 0..10 {
        text.push_str(&format!("{i}\n"));
    }
    text.push_str("not a number\n");
    let late = TempFile::new("late-text.csv", text.as_bytes());
    let reader = OpenOptions::new()
        .infer_rows(5)
        .as_text()
        .open(&late.0)
        .unwrap();
    assert_eq!(reader.map(|b| b.unwrap().num_rows()).sum::<usize>(), 11);
}

#[test]
fn two_columns_of_one_name_are_refused() {
    let file = TempFile::new("twice.csv", b"a,b,a\n1,2,3\n");
    let error = open(&file.0).unwrap_err();
    assert!(
        matches!(&error, FileError::DuplicateColumn(name) if name == "a"),
        "{error:?}"
    );
}

#[test]
fn files_with_no_records_read_as_nothing() {
    let header = TempFile::new("header.csv", b"a,b\n");
    let reader = open(&header.0).unwrap();
    assert_eq!(reader.schema().fields().len(), 2);
    assert_eq!(reader.count(), 0);
    let empty = TempFile::new("empty-file.csv", b"");
    let reader = OpenOptions::new()
        .row_numbers("row")
        .open(&empty.0)
        .unwrap();
    assert_eq!(reader.schema().fields().len(), 1);
    assert_eq!(reader.count(), 0);
}

#[test]
fn types_are_inferred_from_at_least_one_record() {
    let file = TempFile::new("one.csv", ORDERS.as_bytes());
    let schema = OpenOptions::new()
        .infer_rows(0)
        .open(&file.0)
        .unwrap()
        .schema();
    assert_eq!(schema.field(1).data_type(), &DataType::Int64);
}
