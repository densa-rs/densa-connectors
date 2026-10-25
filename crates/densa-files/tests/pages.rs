//! A Parquet file's rows a page at a time (feature `paged`): from anywhere in files of every
//! shape, with or without an offset index, numbered, on several threads, in the grid model;
//! what is refused, and a file written to while open.
#![cfg(feature = "paged")]

use std::fs::{self, File};
use std::ops::Range;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use arrow_array::builder::{Int32Builder, ListBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, UInt64Type};
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::concat::concat_batches;
use densa_arrow::{PageQuery, PageSource, PagedModel};
use densa_files::{FileError, FileFormat, OpenOptions, MAX_BATCH_ROWS};
use densa_grid::{CellText, GridModel, Palette, SortOrder};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::data_type::Int64Type as ParquetInt64;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

/// Rows of the test file, in row groups of `GROUP` rows (300, 300, 300, 100) and pages of
/// `PAGE`.
const ROWS: i64 = 1_000;
const GROUP: usize = 300;
const PAGE: usize = 64;

/// A path in the temporary folder, whatever is there removed when dropped.
struct TempPath(PathBuf);

impl TempPath {
    fn new(name: &str) -> Self {
        let name = format!("densa-files-pages-{}-{name}", std::process::id());
        Self(std::env::temp_dir().join(name))
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
        let _ = fs::remove_dir(&self.0);
    }
}

/// `id` and `name`.
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn rows(ids: Range<i64>) -> RecordBatch {
    let names: Vec<String> = ids.clone().map(|id| format!("row {id}")).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(ids)),
        Arc::new(StringArray::from(names)),
    ];
    RecordBatch::try_new(schema(), columns).unwrap()
}

/// Writes `batch` to `path` in row groups of `GROUP` rows, with an offset index or not.
fn write(path: &Path, batch: &RecordBatch, offset_index: bool) {
    let mut properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(GROUP))
        .set_data_page_row_count_limit(PAGE)
        .set_write_batch_size(PAGE);
    if !offset_index {
        properties = properties
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_offset_index_disabled(true);
    }
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties.build())).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect()
}

/// Windows of the file to read, and the ids they hold: within a row group, across one, at
/// the end, past it, none, all.
fn windows() -> Vec<(u64, usize, Range<i64>)> {
    vec![
        (0, 64, 0..64),
        (250, 100, 250..350),
        (299, 2, 299..301),
        (590, 320, 590..910),
        (600, 400, 600..1_000),
        (950, 100, 950..1_000),
        (1_000, 10, 0..0),
        (5_000, 10, 0..0),
        (0, 0, 0..0),
        (0, 5_000, 0..ROWS),
    ]
}

#[test]
fn rows_come_from_anywhere_in_the_file_a_page_at_a_time() {
    let path = TempPath::new("anywhere.parquet");
    write(&path.0, &rows(0..ROWS), true);
    let mut pages = OpenOptions::new().paged(&path.0).unwrap();
    assert_eq!(pages.total_rows(), 1_000);
    assert!(pages.has_offset_index());
    assert_eq!(pages.schema(), schema());
    assert_eq!(pages.path(), path.0.as_path());
    assert_eq!(pages.row_count(&PageQuery::new()).unwrap(), 1_000);
    for (offset, limit, expected) in windows() {
        let read = pages.read(offset, limit).unwrap();
        assert_eq!(
            ids(&read),
            expected.clone().collect::<Vec<_>>(),
            "{offset} {limit}"
        );
        let paged = pages.page(&PageQuery::new(), offset, limit).unwrap();
        assert_eq!(
            ids(&paged),
            expected.collect::<Vec<_>>(),
            "{offset} {limit}"
        );
    }
}

#[test]
fn a_file_without_an_offset_index_gives_the_same_rows() {
    let path = TempPath::new("no-index.parquet");
    write(&path.0, &rows(0..ROWS), false);
    let pages = OpenOptions::new().paged(&path.0).unwrap();
    assert!(!pages.has_offset_index());
    for (offset, limit, expected) in windows() {
        let read = pages.read(offset, limit).unwrap();
        assert_eq!(ids(&read), expected.collect::<Vec<_>>(), "{offset} {limit}");
    }
}

#[test]
fn row_numbers_count_from_the_start_of_the_file() {
    let path = TempPath::new("numbered.parquet");
    write(&path.0, &rows(0..ROWS), true);
    let pages = OpenOptions::new().row_numbers("#").paged(&path.0).unwrap();
    let schema = pages.schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(2).name(), "#");
    assert_eq!(schema.field(2).data_type(), &DataType::UInt64);
    let read = pages.read(250, 100).unwrap();
    let numbers: Vec<u64> = read
        .iter()
        .flat_map(|batch| {
            batch
                .column(2)
                .as_primitive::<UInt64Type>()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(numbers, (250..350).collect::<Vec<_>>());
    assert_eq!(ids(&read), (250..350).collect::<Vec<_>>());
}

fn cell(model: &PagedModel, row: usize, column: usize) -> String {
    let mut text = CellText::new();
    model.cell(row, column, &mut text, &Palette::new());
    text.as_str().to_owned()
}

/// Paints frames as the grid does, showing `rows`, until `done` holds (5 s at most).
fn frames(model: &mut PagedModel, rows: Range<usize>, done: impl Fn(&PagedModel) -> bool) {
    let start = Instant::now();
    while !done(model) {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "{:?}",
            model.error()
        );
        model.prepare();
        model.showing(rows.clone());
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn the_grid_model_shows_a_files_rows_wherever_it_is_scrolled_but_cannot_sort_them() {
    let path = TempPath::new("model.parquet");
    write(&path.0, &rows(0..ROWS), true);
    let pages = OpenOptions::new().row_numbers("#").paged(&path.0).unwrap();
    let mut model = PagedModel::new(pages.schema(), pages, || {}).unwrap();
    frames(&mut model, 0..20, |model| cell(model, 0, 1) == "row 0");
    assert_eq!(model.rows(), 1_000);
    frames(&mut model, 900..920, |model| cell(model, 917, 2) == "917");
    assert_eq!(cell(&model, 917, 0), "917");
    assert!(
        !model.sort(0, SortOrder::Ascending),
        "the file's order only"
    );
    assert_eq!(model.sort_state(), None);
}

#[test]
fn csv_folders_and_unknown_formats_are_refused_saying_why() {
    let csv = TempPath::new("refused.csv");
    fs::write(&csv.0, "a,b\n1,2\n").unwrap();
    let error = OpenOptions::new().paged(&csv.0).unwrap_err();
    assert!(
        matches!(error, FileError::NotPageable(FileFormat::Csv)),
        "{error:?}"
    );
    let message = error.to_string();
    assert!(
        message.contains("a CSV file cannot be read a page at a time")
            && message.contains("Loader"),
        "{message}"
    );
    let parquet = TempPath::new("given.parquet");
    write(&parquet.0, &rows(0..10), true);
    let given = OpenOptions::new().format(FileFormat::Csv).paged(&parquet.0);
    assert!(matches!(
        given,
        Err(FileError::NotPageable(FileFormat::Csv))
    ));

    let folder = TempPath::new("folder.parquet");
    fs::create_dir_all(&folder.0).unwrap();
    let error = OpenOptions::new().paged(&folder.0).unwrap_err();
    assert!(matches!(error, FileError::Folder(_)), "{error:?}");
    assert!(error
        .to_string()
        .contains("is a folder: open one of its files"));

    let text = TempPath::new("unknown.txt");
    fs::write(&text.0, "text").unwrap();
    let unknown = OpenOptions::new().paged(&text.0);
    assert!(matches!(unknown, Err(FileError::UnknownFormat(_))));

    let missing = OpenOptions::new().paged(TempPath::new("missing.parquet").0.as_path());
    assert!(matches!(missing, Err(FileError::Io(_))));

    let broken = TempPath::new("broken.parquet");
    fs::write(&broken.0, "this is not parquet").unwrap();
    let broken = OpenOptions::new().paged(&broken.0);
    assert!(matches!(broken, Err(FileError::Invalid(_))));

    let taken = OpenOptions::new().row_numbers("id").paged(&parquet.0);
    assert!(matches!(taken, Err(FileError::DuplicateColumn(name)) if name == "id"));
}

#[test]
fn a_file_written_to_is_noticed_and_read_anew_when_counted_again() {
    let path = TempPath::new("rewritten.parquet");
    write(&path.0, &rows(0..ROWS), true);
    let mut pages = OpenOptions::new().paged(&path.0).unwrap();
    assert_eq!(ids(&pages.read(0, 3).unwrap()), [0, 1, 2]);
    write(&path.0, &rows(5_000..5_500), true);
    let error = pages.read(0, 3).unwrap_err();
    assert!(
        matches!(&error, FileError::Changed(changed) if *changed == path.0),
        "{error:?}"
    );
    assert!(error
        .to_string()
        .contains("count them again (refresh the model, or FilePages::reload)"));
    let page = pages.page(&PageQuery::new(), 0, 3).unwrap_err();
    assert!(
        matches!(page.downcast_ref(), Some(FileError::Changed(_))),
        "{page}"
    );
    // Counting again reads the file anew.
    assert_eq!(pages.row_count(&PageQuery::new()).unwrap(), 500);
    assert_eq!(pages.total_rows(), 500);
    assert_eq!(ids(&pages.read(0, 3).unwrap()), [5_000, 5_001, 5_002]);
    assert_eq!(ids(&pages.read(497, 10).unwrap()), [5_497, 5_498, 5_499]);
    // So does `reload`, for a reader of its own, saying whether the file changed.
    assert!(!pages.reload().unwrap(), "read already");
    write(&path.0, &rows(7_000..7_100), true);
    assert!(matches!(pages.read(0, 3), Err(FileError::Changed(_))));
    assert!(pages.reload().unwrap());
    assert!(!pages.reload().unwrap());
    assert_eq!(pages.total_rows(), 100);
    assert_eq!(ids(&pages.read(0, 2).unwrap()), [7_000, 7_001]);
}

/// A file of one column, `id`, holding `ids`: plain values, so as many ids make a file of one
/// length whatever they are.
fn write_ids(path: &Path, ids: Range<i64>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let column: ArrayRef = Arc::new(Int64Array::from_iter_values(ids));
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .build();
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties)).unwrap();
    writer
        .write(&RecordBatch::try_new(schema, vec![column]).unwrap())
        .unwrap();
    writer.close().unwrap();
}

fn modified(path: &Path) -> SystemTime {
    fs::metadata(path).unwrap().modified().unwrap()
}

fn set_modified(path: &Path, time: SystemTime) {
    let file = fs::OpenOptions::new().write(true).open(path).unwrap();
    file.set_modified(time).unwrap();
}

#[test]
fn a_file_rewritten_to_its_old_length_or_its_old_time_is_noticed_too() {
    let path = TempPath::new("stamp.parquet");
    write_ids(&path.0, 0..1_000);
    let pages = OpenOptions::new().paged(&path.0).unwrap();
    let (length, time) = (fs::metadata(&path.0).unwrap().len(), modified(&path.0));
    // Other ids, the same length: only the time moves.
    write_ids(&path.0, 1_000..2_000);
    set_modified(&path.0, time + Duration::from_secs(2));
    assert_eq!(fs::metadata(&path.0).unwrap().len(), length);
    assert!(matches!(pages.read(0, 3), Err(FileError::Changed(_))));
    // Fewer ids written at the old time: only the length moves.
    let pages = OpenOptions::new().paged(&path.0).unwrap();
    let time = modified(&path.0);
    write_ids(&path.0, 0..500);
    set_modified(&path.0, time);
    assert_eq!(modified(&path.0), time);
    assert!(matches!(pages.read(0, 3), Err(FileError::Changed(_))));
}

/// On Unix, another file put in its place is another file, whatever its length and time.
#[cfg(unix)]
#[test]
fn a_file_replaced_by_one_of_its_length_and_time_is_noticed() {
    let path = TempPath::new("replaced.parquet");
    let other = TempPath::new("replacement.parquet");
    write_ids(&path.0, 0..1_000);
    let pages = OpenOptions::new().paged(&path.0).unwrap();
    write_ids(&other.0, 1_000..2_000);
    set_modified(&other.0, modified(&path.0));
    assert_eq!(
        fs::metadata(&other.0).unwrap().len(),
        fs::metadata(&path.0).unwrap().len()
    );
    fs::rename(&other.0, &path.0).unwrap();
    assert!(matches!(pages.read(0, 3), Err(FileError::Changed(_))));
}

#[test]
fn a_file_whose_columns_changed_must_be_opened_again() {
    let names: Vec<String> = (0..10).map(|id| format!("row {id}")).collect();
    let names: ArrayRef = Arc::new(StringArray::from(names));
    let numbers: ArrayRef = Arc::new(Int64Array::from_iter_values(0..10));
    let field = |name: &str, data_type| Field::new(name, data_type, false);
    // Another type, another name, a column fewer, a column that may now hold nulls.
    let others = [
        vec![
            (field("id", DataType::Utf8), names.clone()),
            (field("name", DataType::Utf8), names.clone()),
        ],
        vec![
            (field("ident", DataType::Int64), numbers.clone()),
            (field("name", DataType::Utf8), names.clone()),
        ],
        vec![(field("id", DataType::Int64), numbers.clone())],
        vec![
            (Field::new("id", DataType::Int64, true), numbers),
            (field("name", DataType::Utf8), names.clone()),
        ],
    ];
    let path = TempPath::new("columns.parquet");
    for other in others {
        write(&path.0, &rows(0..ROWS), true);
        let mut pages = OpenOptions::new().paged(&path.0).unwrap();
        let (fields, columns): (Vec<Field>, Vec<ArrayRef>) = other.into_iter().unzip();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        write(&path.0, &batch, true);
        let error = pages.row_count(&PageQuery::new()).unwrap_err();
        let error = error.downcast_ref::<FileError>().unwrap();
        assert!(
            matches!(error, FileError::ColumnsChanged(_)),
            "{:?}: {error:?}",
            batch.schema()
        );
        assert!(error
            .to_string()
            .contains("changed since it was opened: open it again"));
    }
}

#[test]
fn a_file_with_two_columns_of_one_name_is_refused() {
    let path = TempPath::new("twice.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
    ]));
    let column: ArrayRef = Arc::new(Int64Array::from_iter_values(0..10));
    let batch = RecordBatch::try_new(schema, vec![column.clone(), column]).unwrap();
    write(&path.0, &batch, true);
    let twice = OpenOptions::new().paged(&path.0);
    assert!(matches!(twice, Err(FileError::DuplicateColumn(name)) if name == "id"));
}

#[test]
fn the_size_of_a_files_rows_is_told_uncompressed() {
    let path = TempPath::new("size.parquet");
    // One long value, again and again: snappy makes little of it.
    let names: Vec<String> = (0..20_000).map(|_| "x".repeat(100)).collect();
    let column: ArrayRef = Arc::new(StringArray::from(names));
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(schema, vec![column]).unwrap();
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_dictionary_enabled(false)
        .build();
    let file = File::create(&path.0).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let pages = OpenOptions::new().paged(&path.0).unwrap();
    let length = fs::metadata(&path.0).unwrap().len();
    // 2 MB of text in the rows, much less on disk.
    assert!(pages.data_bytes() >= 2_000_000, "{}", pages.data_bytes());
    assert!(length * 4 < pages.data_bytes(), "{length} on disk");
}

#[test]
fn a_file_of_no_rows_has_no_pages() {
    let path = TempPath::new("empty.parquet");
    let writer = ArrowWriter::try_new(File::create(&path.0).unwrap(), schema(), None).unwrap();
    writer.close().unwrap();
    let mut pages = OpenOptions::new().paged(&path.0).unwrap();
    assert_eq!(pages.total_rows(), 0);
    assert_eq!(pages.row_count(&PageQuery::new()).unwrap(), 0);
    assert!(pages.read(0, 10).unwrap().is_empty());
}

/// Columns of every kind a page skip must get right: a required number, an optional one, text
/// of varying length with nulls, a list, a float.
fn mixed_schema() -> SchemaRef {
    let item = Arc::new(Field::new("item", DataType::Int32, true));
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("opt", DataType::Int64, true),
        Field::new("text", DataType::Utf8, true),
        Field::new("list", DataType::List(item), true),
        Field::new("float", DataType::Float64, false),
    ]))
}

fn mixed(ids: Range<usize>) -> RecordBatch {
    let opt = ids.clone().map(|i| (i % 7 != 3).then_some(i as i64 * 10));
    let text = ids
        .clone()
        .map(|i| (i % 11 != 5).then(|| "x".repeat(i % 17) + &i.to_string()));
    let mut list = ListBuilder::new(Int32Builder::new());
    for i in ids.clone() {
        if i % 13 == 4 {
            list.append_null();
        } else {
            for k in 0..i % 5 {
                list.values().append_value((i * 3 + k) as i32);
            }
            list.append(true);
        }
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(ids.clone().map(|i| i as i64))),
        Arc::new(Int64Array::from_iter(opt)),
        Arc::new(StringArray::from_iter(text)),
        Arc::new(list.finish()),
        Arc::new(Float64Array::from_iter_values(ids.map(|i| i as f64 / 3.0))),
    ];
    RecordBatch::try_new(mixed_schema(), columns).unwrap()
}

/// Row groups of uneven sizes, one of a single row.
const MIXED_GROUPS: [usize; 6] = [1, 300, 2, 499, 1, 397];

/// Writes the mixed rows in `MIXED_GROUPS` as the five bits of `shape` say: version 2 pages,
/// an offset index, pages of a few hundred bytes (so each column's pages end at other rows),
/// snappy, dictionaries.
fn write_mixed(path: &Path, shape: u32) {
    let bit = |n: u32| shape & (1 << n) != 0;
    let version = match bit(0) {
        true => WriterVersion::PARQUET_2_0,
        false => WriterVersion::PARQUET_1_0,
    };
    let compression = match bit(3) {
        true => Compression::SNAPPY,
        false => Compression::UNCOMPRESSED,
    };
    let mut properties = WriterProperties::builder()
        .set_writer_version(version)
        .set_compression(compression)
        .set_dictionary_enabled(bit(4));
    if !bit(1) {
        properties = properties
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_offset_index_disabled(true);
    }
    if bit(2) {
        properties = properties
            .set_data_page_size_limit(300)
            .set_write_batch_size(4);
    }
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, mixed_schema(), Some(properties.build())).unwrap();
    let mut at = 0;
    for rows in MIXED_GROUPS {
        writer.write(&mixed(at..at + rows)).unwrap();
        writer.flush().unwrap();
        at += rows;
    }
    writer.close().unwrap();
}

#[test]
fn pages_of_files_of_every_shape_equal_a_read_of_the_whole_file() {
    let rows: usize = MIXED_GROUPS.iter().sum();
    let mut starts = vec![0];
    for size in MIXED_GROUPS {
        starts.push(starts.last().unwrap() + size);
    }
    // Every 53rd row, and the rows about each row group's start.
    let mut offsets: Vec<usize> = (0..rows + 3).step_by(53).collect();
    for start in &starts {
        offsets.extend(start.saturating_sub(2)..start + 3);
    }
    let path = TempPath::new("shapes.parquet");
    for shape in 0..32 {
        write_mixed(&path.0, shape);
        let pages = OpenOptions::new().paged(&path.0).unwrap();
        assert_eq!(pages.has_offset_index(), shape & 2 != 0, "shape {shape}");
        let read: Vec<RecordBatch> = densa_files::open(&path.0)
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let whole = concat_batches(&pages.schema(), &read).unwrap();
        assert_eq!(whole.num_rows(), rows);
        for &offset in &offsets {
            for limit in [1, 7, 64, 300] {
                let got = pages.read(offset as u64, limit).unwrap();
                let got = concat_batches(&pages.schema(), &got).unwrap();
                let from = offset.min(rows);
                let expected = whole.slice(from, limit.min(rows - from));
                let at = format!("shape {shape}, rows {offset}+{limit}");
                assert_eq!(got.num_rows(), expected.num_rows(), "{at}");
                let columns = got.columns().iter().zip(expected.columns());
                for (column, (got, expected)) in columns.enumerate() {
                    assert!(got.as_ref() == expected.as_ref(), "{at}, column {column}");
                }
            }
        }
    }
}

#[test]
fn row_groups_of_no_rows_are_stepped_over() {
    let sizes = [0_i64, 3, 0, 0, 5, 0, 2, 0];
    let schema = Arc::new(parse_message_type("message m { required int64 id; }").unwrap());
    let path = TempPath::new("empty-groups.parquet");
    let properties = WriterProperties::builder()
        .set_data_page_row_count_limit(2)
        .set_write_batch_size(2)
        .build();
    let file = File::create(&path.0).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, Arc::new(properties)).unwrap();
    let mut next = 0;
    for size in sizes {
        let values: Vec<i64> = (next..next + size).collect();
        next += size;
        let mut group = writer.next_row_group().unwrap();
        let mut column = group.next_column().unwrap().unwrap();
        column
            .typed::<ParquetInt64>()
            .write_batch(&values, None, None)
            .unwrap();
        column.close().unwrap();
        group.close().unwrap();
    }
    writer.close().unwrap();
    let pages = OpenOptions::new().paged(&path.0).unwrap();
    assert_eq!(pages.total_rows(), 10);
    for offset in 0..13_u64 {
        for limit in 0..13_usize {
            let end = (offset + limit as u64).min(10);
            let expected: Vec<i64> = (offset as i64..end as i64).collect();
            let got = ids(&pages.read(offset, limit).unwrap());
            assert_eq!(got, expected, "rows {offset}+{limit}");
        }
    }
}

#[test]
fn reads_on_several_threads_at_once_are_each_right() {
    let path = TempPath::new("threads.parquet");
    write(&path.0, &rows(0..ROWS), true);
    let pages = Arc::new(OpenOptions::new().paged(&path.0).unwrap());
    let threads: Vec<_> = (0..4_u64)
        .map(|thread| {
            let pages = Arc::clone(&pages);
            std::thread::spawn(move || {
                let mut seed = 0x9E37_79B9_7F4A_7C15_u64 ^ thread;
                for _ in 0..200 {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    let offset = seed % ROWS as u64;
                    let end = (offset + 64).min(ROWS as u64);
                    let expected: Vec<i64> = (offset as i64..end as i64).collect();
                    assert_eq!(ids(&pages.read(offset, 64).unwrap()), expected);
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
}

#[cfg(windows)]
#[test]
fn the_file_is_not_held_between_reads() {
    use std::os::windows::fs::OpenOptionsExt;
    let path = TempPath::new("not-held.parquet");
    write(&path.0, &rows(0..ROWS), true);
    let pages = OpenOptions::new().paged(&path.0).unwrap();
    assert_eq!(ids(&pages.read(0, 2).unwrap()), [0, 1]);
    // Shared with nobody: refused while any other handle is open.
    let alone = fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(&path.0);
    assert!(alone.is_ok(), "{alone:?}");
    drop(alone);
    assert_eq!(ids(&pages.read(2, 2).unwrap()), [2, 3]);
}

/// `path` as a path relative to the current folder, through its root: `None` on another drive.
fn relative(path: &Path) -> Option<PathBuf> {
    let here = std::env::current_dir().ok()?;
    let root = |path: &Path| -> PathBuf {
        path.components()
            .take_while(|part| matches!(part, Component::Prefix(_) | Component::RootDir))
            .collect()
    };
    if root(&here) != root(path) {
        return None;
    }
    let normal = |part: &Component| matches!(part, Component::Normal(_));
    let ups = here.components().filter(normal).count();
    let mut relative: PathBuf = std::iter::repeat_n("..", ups).collect();
    relative.extend(path.components().filter(normal));
    Some(relative)
}

#[test]
fn a_relative_path_is_made_absolute() {
    let path = TempPath::new("relative.parquet");
    write(&path.0, &rows(0..ROWS), true);
    let Some(relative) = relative(&path.0) else {
        return;
    };
    assert!(relative.is_relative());
    let pages = OpenOptions::new().paged(&relative).unwrap();
    assert!(pages.path().is_absolute(), "{}", pages.path().display());
    assert_eq!(ids(&pages.read(0, 2).unwrap()), [0, 1]);
}

#[test]
fn more_rows_than_a_batch_holds_come_in_several() {
    let path = TempPath::new("batches.parquet");
    let rows = MAX_BATCH_ROWS as i64 + 1_000;
    write_ids(&path.0, 0..rows);
    let pages = OpenOptions::new().paged(&path.0).unwrap();
    let read = pages.read(0, usize::MAX).unwrap();
    assert!(read.len() > 1);
    assert!(read.iter().all(|batch| batch.num_rows() <= MAX_BATCH_ROWS));
    assert_eq!(ids(&read), (0..rows).collect::<Vec<_>>());
}
