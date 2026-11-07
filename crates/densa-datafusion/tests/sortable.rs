//! A Parquet file's rows a page at a time, sorted by any of its columns (feature `paged`): each
//! order checked whole against the rows sorted here, ascending and descending, nulls last and
//! ties in the file's order, for every type and a file read in parts; the file's own order
//! without a copy; pages past the end; one copy kept and removed, copies left behind swept; a
//! file written to; a sort given up; the grid model.
#![cfg(feature = "paged")]

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use arrow_array::builder::{Int32Builder, ListBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, UInt64Type};
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use densa_arrow::{PageQuery, PageSource, PagedModel, SortKey};
use densa_datafusion::{Engine, SortablePages};
use densa_files::{FileError, OpenOptions};
use densa_grid::{CellText, GridModel, Palette, SortOrder};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// Rows of the test file, in row groups of `GROUP` rows.
const ROWS: i64 = 2_000;
const GROUP: usize = 700;

/// A folder of its own in the temporary folder, for a test's file and the copies made of it;
/// removed with all in it when dropped.
struct Folder(PathBuf);

impl Folder {
    fn new(name: &str) -> Self {
        let name = format!("densa-datafusion-sortable-{}-{name}", std::process::id());
        let path = std::env::temp_dir().join(name);
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn file(&self) -> PathBuf {
        self.0.join("data.parquet")
    }

    /// The sorted copies in it.
    fn copies(&self) -> usize {
        fs::read_dir(&self.0)
            .unwrap()
            .filter(|entry| {
                let name = entry.as_ref().unwrap().file_name();
                name.to_string_lossy().starts_with("densa-sorted-")
            })
            .count()
    }
}

impl Drop for Folder {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// The value of row `id`: one of 101, null every 7th row (so many rows share each value).
fn value(id: i64) -> Option<f64> {
    (id % 7 != 0).then(|| ((id * 37) % 101) as f64)
}

/// The name of row `id`: one of 50, null every 11th row.
fn name(id: i64) -> Option<String> {
    (id % 11 != 0).then(|| format!("n{:02}", (id * 13) % 50))
}

/// `id`, `value` and `name` of rows `ids`.
fn rows(ids: std::ops::Range<i64>) -> RecordBatch {
    let values: Float64Array = ids.clone().map(value).collect();
    let names: StringArray = ids.clone().map(name).collect();
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(ids)) as ArrayRef,
        ),
        ("value", Arc::new(values) as ArrayRef),
        ("name", Arc::new(names) as ArrayRef),
    ])
    .unwrap()
}

fn write(path: &Path, batch: &RecordBatch) {
    write_grouped(path, batch, GROUP);
}

/// `batch` written in row groups of `group` rows.
fn write_grouped(path: &Path, batch: &RecordBatch, group: usize) {
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(group))
        .build();
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties)).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

fn engine() -> Engine {
    Engine::with_threads(4).unwrap()
}

/// The file's pages, numbered in `row`, sortable, their copies made in `folder`.
fn sortable(folder: &Folder) -> SortablePages {
    let file = OpenOptions::new()
        .row_numbers("row")
        .paged(folder.file())
        .unwrap();
    engine().sortable(file).unwrap().with_folder(&folder.0)
}

fn by(column: &str, order: SortOrder) -> PageQuery {
    PageQuery::new().sorted_by(SortKey::new(column, order))
}

/// The ids of every row in `query`'s order, read in pages of `page` rows, each batch checked to
/// have the model's columns.
fn all(pages: &mut SortablePages, query: &PageQuery, page: usize) -> Vec<i64> {
    let rows = pages.row_count(query).unwrap();
    let schema = pages.schema();
    let mut ids = Vec::new();
    let mut offset = 0;
    while offset < rows {
        for batch in pages.page(query, offset, page).unwrap() {
            assert_eq!(batch.schema(), schema, "the model's columns");
            ids.extend(batch.column(0).as_primitive::<Int64Type>().values().iter());
        }
        offset += page as u64;
    }
    assert_eq!(ids.len() as u64, rows, "each row once");
    ids
}

/// The ids sorted by `key` ascending, nulls last, ties in the file's order.
fn sorted<K: PartialOrd>(key: impl Fn(i64) -> Option<K>) -> Vec<i64> {
    sorted_of(ROWS, key)
}

/// Ids `0..rows` sorted by `key` ascending, nulls last, ties in the file's order.
fn sorted_of<K: PartialOrd>(rows: i64, key: impl Fn(i64) -> Option<K>) -> Vec<i64> {
    let mut ids: Vec<i64> = (0..rows).collect();
    ids.sort_by(|&a, &b| match (key(a), key(b)) {
        (Some(x), Some(y)) => x.partial_cmp(&y).unwrap().then(a.cmp(&b)),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => a.cmp(&b),
    });
    ids
}

/// The ids descending: the values of `ascending` backwards, then its nulls as they are.
fn descending(ascending: &[i64], is_null: impl Fn(i64) -> bool) -> Vec<i64> {
    let values = ascending.iter().take_while(|&&id| !is_null(id)).count();
    let mut ids: Vec<i64> = ascending[..values].iter().rev().copied().collect();
    ids.extend(&ascending[values..]);
    ids
}

#[test]
fn rows_come_sorted_by_any_column_nulls_last_and_ties_in_the_files_order() {
    let folder = Folder::new("ascending");
    write(&folder.file(), &rows(0..ROWS));
    let mut pages = sortable(&folder);
    assert_eq!(all(&mut pages, &PageQuery::new(), 300), sorted(Some));
    for page in [1, 64, 1_000, 5_000] {
        assert_eq!(
            all(&mut pages, &by("value", SortOrder::Ascending), page),
            sorted(value),
            "pages of {page}"
        );
    }
    assert_eq!(
        all(&mut pages, &by("name", SortOrder::Ascending), 333),
        sorted(name)
    );
    // Each row keeps its number from the file.
    let batches = pages
        .page(&by("value", SortOrder::Ascending), 0, 40)
        .unwrap();
    for batch in &batches {
        let ids = batch.column(0).as_primitive::<Int64Type>();
        let numbers = batch.column(3).as_primitive::<UInt64Type>();
        for (id, number) in ids.values().iter().zip(numbers.values()) {
            assert_eq!(*id as u64, *number);
        }
    }
}

#[test]
fn descending_is_the_copy_read_backwards_nulls_still_last() {
    let folder = Folder::new("descending");
    write(&folder.file(), &rows(0..ROWS));
    let mut pages = sortable(&folder);
    let expected = descending(&sorted(value), |id| value(id).is_none());
    for page in [1, 7, 64, 1_999, 2_000] {
        assert_eq!(
            all(&mut pages, &by("value", SortOrder::Descending), page),
            expected,
            "pages of {page}"
        );
    }
    // A page across the last value and the first null.
    let values = (0..ROWS).filter(|&id| value(id).is_some()).count() as u64;
    let across = pages
        .page(&by("value", SortOrder::Descending), values - 3, 6)
        .unwrap();
    let ids: Vec<i64> = across
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(ids, expected[values as usize - 3..values as usize + 3]);
    // Past the end: nothing.
    assert!(pages
        .page(&by("value", SortOrder::Descending), 2_000, 10)
        .unwrap()
        .is_empty());
}

#[test]
fn the_row_numbers_give_the_files_order_and_its_reverse_without_a_copy() {
    let folder = Folder::new("numbers");
    write(&folder.file(), &rows(0..ROWS));
    let mut pages = sortable(&folder);
    let forward: Vec<i64> = (0..ROWS).collect();
    assert_eq!(
        all(&mut pages, &by("row", SortOrder::Ascending), 300),
        forward
    );
    let backward: Vec<i64> = (0..ROWS).rev().collect();
    assert_eq!(
        all(&mut pages, &by("row", SortOrder::Descending), 300),
        backward
    );
    assert_eq!(folder.copies(), 0);
}

#[test]
fn a_file_without_row_numbers_is_sorted_by_hidden_ones_left_out_of_its_pages() {
    let folder = Folder::new("unnumbered");
    write(&folder.file(), &rows(0..ROWS));
    let file = OpenOptions::new().paged(folder.file()).unwrap();
    // A folder for the copies that is not there yet is made.
    let copies = folder.0.join("copies");
    let mut pages = engine().sortable(file).unwrap().with_folder(&copies);
    assert_eq!(pages.schema().fields().len(), 3);
    assert_eq!(
        all(&mut pages, &by("value", SortOrder::Ascending), 256),
        sorted(value)
    );
    assert_eq!(
        all(&mut pages, &by("name", SortOrder::Descending), 256),
        descending(&sorted(name), |id| name(id).is_none())
    );
}

#[test]
fn one_copy_is_kept_and_removed_with_the_pages() {
    let folder = Folder::new("copies");
    write(&folder.file(), &rows(0..ROWS));
    let wakes = Arc::new(AtomicUsize::new(0));
    let woken = Arc::clone(&wakes);
    let mut pages = sortable(&folder).with_wake(move || {
        woken.fetch_add(1, Ordering::Relaxed);
    });
    pages
        .page(&by("value", SortOrder::Ascending), 0, 10)
        .unwrap();
    assert_eq!(folder.copies(), 1);
    // Woken as the copy began and as it ended.
    let after = wakes.load(Ordering::Relaxed);
    assert!(after >= 2, "{after} wakes");
    // The other way round, the same copy: none made, no wake.
    pages
        .page(&by("value", SortOrder::Descending), 0, 10)
        .unwrap();
    assert_eq!(folder.copies(), 1);
    assert_eq!(wakes.load(Ordering::Relaxed), after);
    pages
        .page(&by("name", SortOrder::Ascending), 0, 10)
        .unwrap();
    assert_eq!(folder.copies(), 1, "replaced");
    drop(pages);
    assert_eq!(folder.copies(), 0);
}

#[test]
fn a_file_written_to_fails_its_pages_until_counted_again_then_is_sorted_anew() {
    let folder = Folder::new("rewritten");
    write(&folder.file(), &rows(0..ROWS));
    let mut pages = sortable(&folder);
    let query = by("value", SortOrder::Ascending);
    assert_eq!(pages.row_count(&query).unwrap(), 2_000);
    pages.page(&query, 0, 10).unwrap();
    write(&folder.file(), &rows(0..500));
    let error = pages.page(&query, 0, 10).unwrap_err();
    assert!(
        matches!(error.downcast_ref(), Some(FileError::Changed(_))),
        "{error}"
    );
    assert_eq!(pages.row_count(&query).unwrap(), 500);
    let ids = all(&mut pages, &query, 128);
    let expected: Vec<i64> = sorted(value).into_iter().filter(|&id| id < 500).collect();
    assert_eq!(ids, expected);
    assert_eq!(folder.copies(), 1, "the old copy went with the old file");
}

#[test]
fn a_sort_the_grid_no_longer_shows_is_given_up() {
    let folder = Folder::new("given-up");
    // Large enough that sorting it takes a while, even in a release build.
    write(&folder.file(), &rows(0..1_500_000));
    // A wake that panics stops nothing.
    let mut pages = sortable(&folder).with_wake(|| panic!("a wake that panics"));
    let sorts = pages.sorts();
    let sorting = std::thread::spawn(move || {
        let result = pages.page(&by("value", SortOrder::Ascending), 0, 10);
        (result.map(|_| ()).map_err(|error| error.to_string()), pages)
    });
    let start = Instant::now();
    while sorts.making().is_none() {
        assert!(start.elapsed() < Duration::from_secs(30), "never began");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(sorts.making().unwrap().column(), "value");
    sorts.showing(Some("name"));
    let (result, pages) = sorting.join().unwrap();
    let error = result.unwrap_err();
    assert!(error.contains("sorting by value was given up"), "{error}");
    assert!(sorts.making().is_none());
    drop(pages);
    assert_eq!(folder.copies(), 0, "no copy left behind");
}

#[test]
fn nested_columns_cannot_be_sorted_and_a_pattern_in_the_folder_is_refused() {
    let folder = Folder::new("refused");
    let mut lists = ListBuilder::new(Int32Builder::new());
    for id in 0..10 {
        lists.values().append_value(id);
        lists.append(true);
    }
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..10)) as ArrayRef,
        ),
        ("list", Arc::new(lists.finish()) as ArrayRef),
    ])
    .unwrap();
    write(&folder.file(), &batch);
    let file = OpenOptions::new()
        .row_numbers("row")
        .paged(folder.file())
        .unwrap();
    let pages = engine().sortable(file).unwrap();
    assert!(pages.can_sort("id") && pages.can_sort("row"));
    assert!(!pages.can_sort("list") && !pages.can_sort("missing"));
    let patterned = folder.0.join("copies [1]");
    fs::create_dir_all(&patterned).unwrap();
    let mut pages = pages.with_folder(&patterned);
    let error = pages
        .page(&by("id", SortOrder::Ascending), 0, 5)
        .unwrap_err()
        .to_string();
    assert!(error.contains("as a pattern"), "{error}");
    assert!(error.contains("SortablePages::with_folder"), "{error}");
}

fn cell(model: &PagedModel, row: usize, column: usize) -> String {
    let mut text = CellText::new();
    model.cell(row, column, &mut text, &Palette::new());
    text.as_str().to_owned()
}

/// Paints frames as the grid does, showing `rows`, until `done` holds (20 s at most).
fn frames(
    model: &mut PagedModel,
    rows: std::ops::Range<usize>,
    done: impl Fn(&PagedModel) -> bool,
) {
    let start = Instant::now();
    while !done(model) {
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "{:?}",
            model.error()
        );
        model.prepare();
        model.showing(rows.clone());
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn the_grid_model_sorts_a_file_by_a_click_on_a_title() {
    let folder = Folder::new("model");
    write(&folder.file(), &rows(0..ROWS));
    let pages = sortable(&folder);
    let mut model = PagedModel::new(pages.schema(), pages, || {}).unwrap();
    frames(&mut model, 0..20, |model| cell(model, 0, 0) == "0");
    assert!(model.sort(1, SortOrder::Ascending));
    let first = sorted(value)[0];
    frames(&mut model, 0..20, |model| {
        cell(model, 0, 0) == first.to_string()
    });
    assert_eq!(cell(&model, 0, 3), first.to_string(), "its own row number");
    assert!(model.sort(1, SortOrder::Descending));
    let first = descending(&sorted(value), |id| value(id).is_none())[0];
    frames(&mut model, 0..20, |model| {
        cell(model, 0, 0) == first.to_string()
    });
}

#[test]
fn pages_past_the_end_are_empty_in_either_order() {
    let folder = Folder::new("past-the-end");
    write(&folder.file(), &rows(0..ROWS));
    let mut pages = sortable(&folder);
    for column in ["value", "row"] {
        for order in [SortOrder::Ascending, SortOrder::Descending] {
            let query = by(column, order);
            for (offset, limit) in [(2_000, 10), (2_001, 1), (u64::MAX - 5, 10), (5, 0)] {
                let page = pages.page(&query, offset, limit).unwrap();
                assert!(page.is_empty(), "{column} {order:?} from {offset}");
            }
            let last = pages.page(&query, 1_999, 10).unwrap();
            let rows: usize = last.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(rows, 1, "{column} {order:?}: the last row");
        }
    }
}

/// A file large enough that DataFusion reads it in parts side by side (it splits files of more
/// than 10 MiB by default): sorted whole all the same, each row numbered from the file.
#[test]
fn a_file_read_in_parts_side_by_side_is_sorted_whole() {
    const BIG: i64 = 400_000;
    let folder = Folder::new("parts");
    let ids = rows(0..BIG);
    let pad: StringArray = (0..BIG)
        .map(|id| Some(format!("{:032}", id * 2_654_435_761 % 1_000_000_007)))
        .collect();
    let mut columns = ids.columns().to_vec();
    columns.push(Arc::new(pad));
    let mut fields: Vec<_> = ids.schema().fields().iter().cloned().collect();
    fields.push(Arc::new(arrow_schema::Field::new(
        "pad",
        arrow_schema::DataType::Utf8,
        true,
    )));
    let schema = Arc::new(arrow_schema::Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    write_grouped(&folder.file(), &batch, 50_000);
    assert!(
        fs::metadata(folder.file()).unwrap().len() > 10 << 20,
        "large enough to split"
    );
    let mut pages = sortable(&folder);
    let ascending = sorted_of(BIG, value);
    assert_eq!(
        all(&mut pages, &by("value", SortOrder::Ascending), 65_536),
        ascending
    );
    assert_eq!(
        all(&mut pages, &by("value", SortOrder::Descending), 65_536),
        descending(&ascending, |id| value(id).is_none())
    );
    let numbered = pages
        .page(&by("value", SortOrder::Ascending), 300_000, 1_000)
        .unwrap();
    for batch in &numbered {
        let ids = batch.column(0).as_primitive::<Int64Type>();
        let numbers = batch.column(4).as_primitive::<UInt64Type>();
        for (id, number) in ids.values().iter().zip(numbers.values()) {
            assert_eq!(*id as u64, *number);
        }
    }
}

#[test]
fn every_column_keeps_its_type_in_a_sorted_copy() {
    use arrow_array::types::Int32Type;
    use arrow_array::{
        Date64Array, Decimal128Array, DictionaryArray, FixedSizeBinaryArray, StringViewArray,
        TimestampMillisecondArray,
    };

    const N: i64 = 300;
    let folder = Folder::new("types");
    let views: StringViewArray = (0..N).map(|i| Some(format!("v{}", i * 7 % 13))).collect();
    let words: DictionaryArray<Int32Type> =
        (0..N).map(|i| ["c", "a", "b"][(i % 3) as usize]).collect();
    let times =
        TimestampMillisecondArray::from_iter_values((0..N).map(|i| 1_700_000_000_000 - i * 1_000))
            .with_timezone("UTC");
    let money = Decimal128Array::from_iter_values((0..N).map(|i| i128::from(i * 37 % 101) * 100))
        .with_precision_and_scale(10, 2)
        .unwrap();
    let dates = Date64Array::from_iter_values((0..N).map(|i| (N - i) * 86_400_000));
    let bytes =
        FixedSizeBinaryArray::try_from_iter((0..N).map(|i| (i as u32).to_le_bytes())).unwrap();
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..N)) as ArrayRef,
        ),
        ("views", Arc::new(views) as ArrayRef),
        ("words", Arc::new(words) as ArrayRef),
        ("time", Arc::new(times) as ArrayRef),
        ("money", Arc::new(money) as ArrayRef),
        ("date", Arc::new(dates) as ArrayRef),
        ("bytes", Arc::new(bytes) as ArrayRef),
    ])
    .unwrap();
    write(&folder.file(), &batch);
    let mut pages = sortable(&folder);
    // `all` checks each page has the model's columns, types and all.
    let by_words = sorted_of(N, |i| Some(["c", "a", "b"][(i % 3) as usize]));
    assert_eq!(
        all(&mut pages, &by("words", SortOrder::Ascending), 128),
        by_words
    );
    let by_views = sorted_of(N, |i| Some(format!("v{}", i * 7 % 13)));
    assert_eq!(
        all(&mut pages, &by("views", SortOrder::Ascending), 128),
        by_views
    );
    let latest: Vec<i64> = (0..N).collect();
    assert_eq!(
        all(&mut pages, &by("time", SortOrder::Descending), 128),
        latest
    );
    let by_money = sorted_of(N, |i| Some(i * 37 % 101));
    assert_eq!(
        all(&mut pages, &by("money", SortOrder::Ascending), 128),
        by_money
    );
    let earliest: Vec<i64> = (0..N).rev().collect();
    assert_eq!(
        all(&mut pages, &by("date", SortOrder::Ascending), 128),
        earliest
    );
    let by_bytes = sorted_of(N, |i| Some((i as u32).to_le_bytes()));
    assert_eq!(
        all(&mut pages, &by("bytes", SortOrder::Ascending), 128),
        by_bytes
    );
}

/// A copy's folder in `folder` as one left behind would be: a partial copy and its lock, last
/// written at `modified`.
fn left_behind(folder: &Folder, name: &str, modified: SystemTime) -> PathBuf {
    let path = folder.0.join(name);
    fs::create_dir(&path).unwrap();
    fs::write(path.join("copy.parquet"), b"partial").unwrap();
    let lock = File::create(path.join("lock")).unwrap();
    lock.set_modified(modified).unwrap();
    path
}

#[test]
fn folders_of_copies_no_process_holds_are_swept_by_the_next_sort() {
    let folder = Folder::new("sweep");
    write(&folder.file(), &rows(0..ROWS));
    let old = SystemTime::now() - Duration::from_secs(120);
    // Left by a process that is gone: its lock free, and old.
    let gone = left_behind(&folder, "densa-sorted-1-1", old);
    // Held by a process still running (this test).
    let held = left_behind(&folder, "densa-sorted-1-2", old);
    let lock = File::options().write(true).open(held.join("lock")).unwrap();
    lock.lock().unwrap();
    // Just made: its owner may not have locked it yet.
    let young = left_behind(&folder, "densa-sorted-1-3", SystemTime::now());
    // Not a copy's, by its name or by what it holds: never removed.
    let other = left_behind(&folder, "other", old);
    let named = left_behind(&folder, "densa-sorted-results", old);
    let holding = left_behind(&folder, "densa-sorted-1-4", old);
    fs::write(holding.join("notes.txt"), b"mine").unwrap();
    let mut pages = sortable(&folder);
    pages
        .page(&by("value", SortOrder::Ascending), 0, 10)
        .unwrap();
    assert!(!gone.exists(), "swept");
    assert!(held.exists() && young.exists(), "kept");
    for kept in [&other, &named, &holding] {
        let untouched = kept.join("lock").exists() && kept.join("copy.parquet").exists();
        assert!(untouched, "not a copy's: {}", kept.display());
    }
    drop(lock);
    drop(pages);
    assert_eq!(folder.copies(), 4, "only its own copy went with the pages");
}

#[cfg(unix)]
#[test]
fn a_copys_folder_is_its_owners_alone() {
    use std::os::unix::fs::PermissionsExt;

    let folder = Folder::new("private");
    write(&folder.file(), &rows(0..ROWS));
    let mut pages = sortable(&folder);
    pages
        .page(&by("value", SortOrder::Ascending), 0, 10)
        .unwrap();
    let copies: Vec<PathBuf> = fs::read_dir(&folder.0)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            let name = path.file_name().unwrap().to_string_lossy();
            name.starts_with("densa-sorted-")
        })
        .collect();
    assert_eq!(copies.len(), 1);
    let mode = fs::metadata(&copies[0]).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o700);
}

/// A page of more values than a batch holds (densa-files' `MAX_BATCH_ROWS`) is read in several
/// batches: descending, each is turned around and their order too.
#[test]
fn a_page_of_more_rows_than_a_batch_holds_comes_backwards_whole() {
    const MANY: i64 = 1_400_000;
    let values = (0..MANY).filter(|&id| value(id).is_some()).count();
    assert!(
        values > densa_files::MAX_BATCH_ROWS,
        "values alone fill more than a batch"
    );
    let folder = Folder::new("many");
    let values: Float64Array = (0..MANY).map(value).collect();
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..MANY)) as ArrayRef,
        ),
        ("value", Arc::new(values) as ArrayRef),
    ])
    .unwrap();
    write_grouped(&folder.file(), &batch, 300_000);
    let mut pages = sortable(&folder);
    let query = by("value", SortOrder::Descending);
    let page = pages.page(&query, 0, MANY as usize).unwrap();
    assert!(page.len() > 1, "in several batches");
    let ids: Vec<i64> = page
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(
        ids,
        descending(&sorted_of(MANY, value), |id| value(id).is_none())
    );
}
