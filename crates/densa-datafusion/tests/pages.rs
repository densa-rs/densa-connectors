//! A query's rows a page at a time (feature `paged`): counted, paged in the grid's order with
//! keys breaking ties, a file's own order kept without keys, and refused when it cannot be
//! planned or a key is not one of its columns.
#![cfg(feature = "paged")]

use std::fs::File;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use densa_arrow::{PageQuery, PageSource, PagedModel, SortKey};
use densa_datafusion::{Engine, EngineError};
use densa_grid::{CellText, GridModel, Palette, SortOrder};

/// Rows of `t`.
const ROWS: i64 = 1_000;

/// `id` (0, 1, ...), `grp` (`g` and the id's remainder by 7) and `value` (the id's last digit:
/// a hundred rows tie on each).
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("grp", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

fn rows(ids: std::ops::Range<i64>) -> RecordBatch {
    let grp: Vec<String> = ids.clone().map(|id| format!("g{}", id % 7)).collect();
    let value: Vec<f64> = ids.clone().map(|id| (id % 10) as f64).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(ids)) as ArrayRef,
            Arc::new(StringArray::from(grp)),
            Arc::new(Float64Array::from(value)),
        ],
    )
    .unwrap()
}

/// An engine with `t`: the rows in ten batches, spread over its threads.
fn engine() -> Engine {
    let engine = Engine::with_threads(4).unwrap();
    let batches: Vec<RecordBatch> = (0..10).map(|i| rows(i * 100..(i + 1) * 100)).collect();
    engine.register_batches("t", &schema(), &batches).unwrap();
    engine
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

fn values(batches: &[RecordBatch]) -> Vec<f64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(2)
                .as_primitive::<Float64Type>()
                .values()
                .to_vec()
        })
        .collect()
}

fn by(column: &str, order: SortOrder) -> PageQuery {
    PageQuery::new().sorted_by(SortKey::new(column, order))
}

#[test]
fn a_query_is_counted_and_paged_in_its_keys_order() {
    let mut pages = engine().pages("SELECT * FROM t", &["id"]).unwrap();
    assert_eq!(pages.schema().fields().len(), 3);
    assert_eq!(pages.row_count(&PageQuery::new()).unwrap(), 1_000);
    let first = pages.page(&PageQuery::new(), 0, 10).unwrap();
    assert_eq!(ids(&first), (0..10).collect::<Vec<_>>());
    // Fewer at the end, none past it.
    let last = pages.page(&PageQuery::new(), 990, 50).unwrap();
    assert_eq!(ids(&last), (990..1_000).collect::<Vec<_>>());
    assert!(pages.page(&PageQuery::new(), 1_000, 10).unwrap().is_empty());
}

#[test]
fn rows_that_tie_in_the_sort_come_in_the_keys_order() {
    let mut pages = engine().pages("SELECT * FROM t", &["id"]).unwrap();
    // A hundred rows have each value: the nines first, by id, then the eights.
    let query = by("value", SortOrder::Descending);
    let page = pages.page(&query, 95, 10).unwrap();
    assert_eq!(ids(&page), [959, 969, 979, 989, 999, 8, 18, 28, 38, 48]);
    assert_eq!(values(&page)[..6], [9.0, 9.0, 9.0, 9.0, 9.0, 8.0]);
}

#[test]
fn pages_of_any_size_put_the_whole_result_together_once() {
    let mut pages = engine().pages("SELECT * FROM t", &["id"]).unwrap();
    let query = by("grp", SortOrder::Ascending);
    let whole = ids(&pages.page(&query, 0, 2_000).unwrap());
    for size in [7, 64, 333] {
        let mut together = Vec::new();
        let mut offset = 0;
        while offset < 1_000 {
            together.extend(ids(&pages.page(&query, offset, size).unwrap()));
            offset += size as u64;
        }
        assert_eq!(together, whole, "pages of {size}");
    }
    let mut seen = whole.clone();
    seen.sort_unstable();
    assert_eq!(seen, (0..ROWS).collect::<Vec<_>>(), "each row once");
}

#[test]
fn a_file_without_keys_is_paged_in_its_own_order() {
    let dir = std::env::temp_dir().join(format!("densa-datafusion-pages-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("t.parquet");
    // Four row groups.
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(250))
        .build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        File::create(&path).unwrap(),
        schema(),
        Some(properties),
    )
    .unwrap();
    writer.write(&rows(0..ROWS)).unwrap();
    writer.close().unwrap();

    let engine = Engine::with_threads(4).unwrap();
    engine.register_parquet("f", &path).unwrap();
    let mut pages = engine.pages("SELECT * FROM f", &[]).unwrap();
    let mut together = Vec::new();
    for offset in (0..1_000).step_by(90) {
        together.extend(ids(&pages.page(&PageQuery::new(), offset, 90).unwrap()));
    }
    assert_eq!(together, (0..ROWS).collect::<Vec<_>>());
    // The engine keeps its tables, and sees one registered after.
    assert!(engine.schema("SELECT * FROM f").is_ok());
    engine.register_parquet("g", &path).unwrap();
    let mut later = engine.pages("SELECT * FROM g", &[]).unwrap();
    assert_eq!(later.row_count(&PageQuery::new()).unwrap(), 1_000);
    assert_eq!(pages.row_count(&PageQuery::new()).unwrap(), 1_000);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_folder_of_files_needs_keys_and_with_them_pages_each_row_once() {
    let dir = std::env::temp_dir().join(format!("densa-datafusion-folder-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    for (part, ids) in [(0, 0..400), (1, 400..700), (2, 700..ROWS)] {
        let file = File::create(dir.join(format!("part-{part}.parquet"))).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema(), None).unwrap();
        writer.write(&rows(ids)).unwrap();
        writer.close().unwrap();
    }
    let engine = Engine::with_threads(4).unwrap();
    engine.register_parquet("d", &dir).unwrap();
    // The files are listed in no set order (concurrently): one query may read them in another
    // than the next. So too when the scan is below another step (a filter).
    for query in ["SELECT * FROM d", "SELECT * FROM d WHERE id % 2 = 0"] {
        let reason = match engine.pages(query, &[]) {
            Err(EngineError::Query(reason)) => reason,
            other => panic!("{query}: {other:?}"),
        };
        assert!(
            reason.contains("3 files, listed in no set order") && reason.contains("give keys"),
            "{query}: {reason}"
        );
    }
    let mut pages = engine.pages("SELECT * FROM d", &["id"]).unwrap();
    let mut together = Vec::new();
    for offset in (0..1_000).step_by(70) {
        together.extend(ids(&pages.page(&PageQuery::new(), offset, 70).unwrap()));
    }
    assert_eq!(together, (0..ROWS).collect::<Vec<_>>());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_query_that_cannot_be_planned_or_a_key_not_among_its_columns_is_refused() {
    let engine = engine();
    let refused = |query: &str, keys: &[&str]| match engine.pages(query, keys) {
        Err(EngineError::Query(reason)) => reason,
        other => panic!("{query}: {other:?}"),
    };
    assert!(refused("SELECT nope FROM t", &[]).contains("nope"));
    let reason = refused("SELECT * FROM t", &["key"]);
    assert_eq!(
        reason,
        "no column key to tell rows apart: give keys among id, grp, value"
    );
    // Only reading.
    refused("DROP TABLE t", &[]);
    assert!(
        engine.pages("SELECT * FROM t", &["id"]).is_ok(),
        "t is still there"
    );
}

#[test]
fn a_query_ending_in_a_comment_or_semicolon_is_taken() {
    let mut pages = engine()
        .pages(
            "SELECT * FROM t WHERE id < 30 -- the first thirty\n; ",
            &["id"],
        )
        .unwrap();
    assert_eq!(pages.row_count(&PageQuery::new()).unwrap(), 30);
    assert_eq!(
        ids(&pages.page(&PageQuery::new(), 25, 10).unwrap()),
        [25, 26, 27, 28, 29]
    );
}

#[test]
fn a_table_gone_since_fails_the_calls_that_read_it() {
    let engine = engine();
    let mut pages = engine.pages("SELECT * FROM t", &["id"]).unwrap();
    engine.deregister("t").unwrap();
    let error = pages.row_count(&PageQuery::new()).unwrap_err().to_string();
    assert!(
        error.contains("table 'datafusion.public.t' not found"),
        "{error}"
    );
    assert!(pages.page(&PageQuery::new(), 0, 10).is_err());
}

#[test]
fn without_keys_a_query_read_in_parallel_is_refused() {
    // `t` is ten batches spread over four partitions: read side by side, in no set order, even
    // when a step above gathers them into one (a LIMIT of the query's own).
    for query in ["SELECT * FROM t", "SELECT * FROM t LIMIT 500"] {
        let reason = match engine().pages(query, &[]) {
            Err(EngineError::Query(reason)) => reason,
            other => panic!("{query}: {other:?}"),
        };
        assert!(
            reason.contains("4 partitions") && reason.contains("give keys"),
            "{query}: {reason}"
        );
    }
    // One batch, one partition: its own order holds.
    let engine = Engine::with_threads(4).unwrap();
    engine
        .register_batches("one", &schema(), &[rows(0..ROWS)])
        .unwrap();
    let mut pages = engine.pages("SELECT * FROM one", &[]).unwrap();
    let mut together = Vec::new();
    for offset in (0..1_000).step_by(64) {
        together.extend(ids(&pages.page(&PageQuery::new(), offset, 64).unwrap()));
    }
    assert_eq!(together, (0..ROWS).collect::<Vec<_>>());
}

#[test]
fn the_querys_own_order_by_is_refused_but_one_picking_rows_is_kept() {
    let engine = engine();
    let reason = match engine.pages("SELECT * FROM t ORDER BY value DESC", &["id"]) {
        Err(EngineError::Query(reason)) => reason,
        other => panic!("{other:?}"),
    };
    assert!(
        reason.contains("ORDER BY (t.value DESC") && reason.contains("sort the model"),
        "{reason}"
    );
    // By a column not selected: the order under the projection.
    match engine.pages("SELECT id FROM t ORDER BY value", &["id"]) {
        Err(EngineError::Query(reason)) => assert!(reason.contains("sort the model"), "{reason}"),
        other => panic!("{other:?}"),
    }
    // Under a LIMIT the order picks the rows, and stays: the ten nines, by id.
    let mut top = engine
        .pages("SELECT * FROM t ORDER BY value DESC, id LIMIT 10", &["id"])
        .unwrap();
    assert_eq!(top.row_count(&PageQuery::new()).unwrap(), 10);
    let page = top.page(&PageQuery::new(), 0, 20).unwrap();
    assert_eq!(ids(&page), [9, 19, 29, 39, 49, 59, 69, 79, 89, 99]);
}

#[test]
fn a_query_that_cannot_be_wrapped_is_refused_saying_why() {
    let engine = engine();
    for query in ["SELECT * FROM t; -- done", "EXPLAIN SELECT * FROM t"] {
        match engine.pages(query, &[]) {
            Err(EngineError::Query(reason)) => assert!(
                reason.contains("must be one SELECT (WITH .. SELECT too) with nothing after it"),
                "{query}: {reason}"
            ),
            other => panic!("{query}: {other:?}"),
        }
    }
}

#[test]
fn a_page_needing_more_memory_than_allowed_fails_saying_so() {
    let mut pages = engine()
        .pages("SELECT * FROM t", &["id"])
        .unwrap()
        .with_memory_limit(4 << 10)
        .unwrap();
    let query = by("value", SortOrder::Descending);
    let error = pages.page(&query, 900, 10).unwrap_err().to_string();
    assert!(
        error.contains("needs more than the 4096 bytes allowed (SqlPages::with_memory_limit)"),
        "{error}"
    );
    // An unordered page streams past the rows before it.
    let scan = Engine::with_threads(1).unwrap();
    scan.register_batches("one", &schema(), &[rows(0..ROWS)])
        .unwrap();
    let mut scan = scan
        .pages("SELECT * FROM one", &[])
        .unwrap()
        .with_memory_limit(4 << 10)
        .unwrap();
    assert_eq!(
        ids(&scan.page(&PageQuery::new(), 900, 3).unwrap()),
        [900, 901, 902]
    );
}

#[test]
fn a_table_registered_again_is_seen_by_the_next_call() {
    let engine = engine();
    let mut pages = engine.pages("SELECT * FROM t", &["id"]).unwrap();
    assert_eq!(pages.row_count(&PageQuery::new()).unwrap(), 1_000);
    engine.deregister("t").unwrap();
    engine
        .register_batches("t", &schema(), &[rows(0..500)])
        .unwrap();
    assert_eq!(pages.row_count(&PageQuery::new()).unwrap(), 500);
}

#[test]
fn nested_columns_are_not_sorted_by() {
    let pages = engine()
        .pages("SELECT id, make_array(id, id) AS pair FROM t", &["id"])
        .unwrap();
    assert!(pages.can_sort("id"));
    assert!(!pages.can_sort("pair"));
    assert!(!pages.can_sort("missing"));
}

/// The text of a cell.
fn cell(model: &PagedModel, row: usize, column: usize) -> String {
    let mut text = CellText::new();
    model.cell(row, column, &mut text, &Palette::new());
    text.as_str().to_owned()
}

/// Paints frames as the grid does until `done` holds (5 s at most).
fn frames(
    model: &mut PagedModel,
    rows: std::ops::Range<usize>,
    done: impl Fn(&PagedModel) -> bool,
) {
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
fn the_grid_model_shows_the_pages_and_sorts_at_the_engine() {
    let pages = engine().pages("SELECT * FROM t", &["id"]).unwrap();
    let mut model = PagedModel::new(pages.schema(), pages, || {}).unwrap();
    frames(&mut model, 0..20, |m| m.is_fetched(19));
    assert_eq!(model.row_count(), Some(1_000));
    assert_eq!(cell(&model, 19, 0), "19");
    assert!(model.sort(0, SortOrder::Descending));
    frames(&mut model, 0..20, |m| m.is_fetched(0));
    assert_eq!(cell(&model, 0, 0), "999");
    assert_eq!(model.error(), None);
}
