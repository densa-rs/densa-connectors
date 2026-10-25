//! Queries over registered batches, Parquet and CSV files: results handed over as densa's
//! Arrow, every type crossing from DataFusion's Arrow unchanged, errors as events, and
//! queries cancelled when dropped.

use std::fs::File;
use std::io::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::builder::{Int32Builder, ListBuilder, StringDictionaryBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int32Type, Int64Type, UInt64Type};
use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
    LargeStringArray, RecordBatch, StringArray, StructArray, TimestampMillisecondArray,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use densa_datafusion::{Engine, EngineError, QueryEvent, QueryStream};

const WAIT: Duration = Duration::from_secs(30);

fn readings() -> (SchemaRef, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("sensor", DataType::Utf8, false),
        Field::new("site", DataType::Utf8, false),
        Field::new("celsius", DataType::Float64, true),
    ]));
    let batch = |sensors: &[&str], sites: &[&str], celsius: &[f64]| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(sensors.to_vec())) as ArrayRef,
                Arc::new(StringArray::from(sites.to_vec())),
                Arc::new(Float64Array::from(celsius.to_vec())),
            ],
        )
        .unwrap()
    };
    let batches = vec![
        batch(&["a", "b"], &["north", "south"], &[20.0, 30.0]),
        batch(
            &["c", "d", "e"],
            &["north", "south", "north"],
            &[22.0, 34.0, 24.0],
        ),
    ];
    (schema, batches)
}

/// Everything a query hands over, to its end: the schema, the batches and how it ended.
fn collect(mut stream: QueryStream) -> (Option<SchemaRef>, Vec<RecordBatch>, Result<(), String>) {
    let (mut schema, mut batches) = (None, Vec::new());
    let until = Instant::now() + WAIT;
    while Instant::now() < until {
        match stream.wait_next(WAIT) {
            Some(QueryEvent::Schema(s)) => schema = Some(s),
            Some(QueryEvent::Batch(batch)) => batches.push(batch),
            Some(QueryEvent::Finished) => {
                assert!(stream.is_finished());
                return (schema, batches, Ok(()));
            }
            Some(QueryEvent::Failed { reason }) => return (schema, batches, Err(reason)),
            Some(other) => panic!("{other:?}"),
            None => break,
        }
    }
    panic!("no end within {WAIT:?}");
}

fn concat(schema: &SchemaRef, batches: &[RecordBatch]) -> RecordBatch {
    arrow_select::concat::concat_batches(schema, batches).unwrap()
}

#[test]
fn a_query_over_registered_batches_hands_over_its_schema_rows_and_end() {
    let engine = Engine::with_threads(2).unwrap();
    let (schema, batches) = readings();
    engine
        .register_batches("readings", &schema, &batches)
        .unwrap();
    let query = "SELECT site, count(*) AS sensors, avg(celsius) AS celsius \
                 FROM readings GROUP BY site ORDER BY site";
    let (schema, batches, end) = collect(engine.sql(query).run(|| {}));
    end.unwrap();
    let schema = schema.unwrap();
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["site", "sensors", "celsius"]);
    let result = concat(&schema, &batches);
    let sites: Vec<_> = result
        .column(0)
        .as_string::<i32>()
        .iter()
        .flatten()
        .collect();
    assert_eq!(sites, ["north", "south"]);
    assert_eq!(
        result.column(1).as_primitive::<Int64Type>().values(),
        &[3, 2]
    );
    assert_eq!(
        result.column(2).as_primitive::<Float64Type>().values(),
        &[22.0, 32.0]
    );
}

#[test]
fn parquet_and_csv_files_are_queried_together() {
    let dir = std::env::temp_dir().join(format!("densa-datafusion-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (schema, batches) = readings();
    let parquet = dir.join("readings.parquet");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(File::create(&parquet).unwrap(), schema, None)
            .unwrap();
    for batch in &batches {
        writer.write(batch).unwrap();
    }
    writer.close().unwrap();
    let csv = dir.join("sites.csv");
    let mut file = File::create(&csv).unwrap();
    writeln!(file, "site,region\nnorth,highlands\nsouth,coast").unwrap();
    drop(file);

    let engine = Engine::with_threads(2).unwrap();
    engine.register_parquet("readings", &parquet).unwrap();
    engine.register_csv("sites", &csv).unwrap();
    let query = "SELECT region, max(celsius) AS hottest FROM readings \
                 JOIN sites USING (site) GROUP BY region ORDER BY region";
    let (schema, batches, end) = collect(engine.sql(query).run(|| {}));
    end.unwrap();
    let result = concat(&schema.unwrap(), &batches);
    let regions: Vec<_> = result.column(0).as_string_view_or_string();
    assert_eq!(regions, ["coast", "highlands"]);
    assert_eq!(
        result.column(1).as_primitive::<Float64Type>().values(),
        &[34.0, 24.0]
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// Strings of a column, whichever string type the engine chose.
trait AnyStrings {
    fn as_string_view_or_string(&self) -> Vec<String>;
}

impl AnyStrings for ArrayRef {
    fn as_string_view_or_string(&self) -> Vec<String> {
        match self.data_type() {
            DataType::Utf8View => self
                .as_string_view()
                .iter()
                .flatten()
                .map(str::to_owned)
                .collect(),
            DataType::LargeUtf8 => self
                .as_string::<i64>()
                .iter()
                .flatten()
                .map(str::to_owned)
                .collect(),
            _ => self
                .as_string::<i32>()
                .iter()
                .flatten()
                .map(str::to_owned)
                .collect(),
        }
    }
}

#[test]
fn every_kind_of_column_crosses_unchanged() {
    let mut dictionary = StringDictionaryBuilder::<Int32Type>::new();
    for word in ["red", "blue", "red"] {
        dictionary.append_value(word);
    }
    let mut lists = ListBuilder::new(Int32Builder::new());
    lists.append_value([Some(1), Some(2)]);
    lists.append_null();
    lists.append_value([Some(3)]);
    let point = StructArray::from(vec![
        (
            Arc::new(Field::new("x", DataType::Float64, false)),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("y", DataType::Float64, false)),
            Arc::new(Float64Array::from(vec![4.0, 5.0, 6.0])) as ArrayRef,
        ),
    ]);
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("id", Arc::new(UInt64Array::from(vec![1, 2, 3]))),
        (
            "name",
            Arc::new(LargeStringArray::from(vec![Some("a"), None, Some("c")])),
        ),
        (
            "flag",
            Arc::new(BooleanArray::from(vec![true, false, true])),
        ),
        (
            "day",
            Arc::new(Date32Array::from(vec![20_000, 20_001, 20_002])),
        ),
        (
            "at",
            Arc::new(
                TimestampMillisecondArray::from(vec![1_700_000_000_000, 1_700_000_000_001, 0])
                    .with_timezone("+00:00"),
            ),
        ),
        (
            "price",
            Arc::new(
                Decimal128Array::from(vec![12_345, -1, 0])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
        ),
        ("colour", Arc::new(dictionary.finish())),
        ("tags", Arc::new(lists.finish())),
        ("point", Arc::new(point)),
        (
            "count",
            Arc::new(Int64Array::from(vec![Some(7), None, Some(9)])),
        ),
    ];
    let fields: Fields = columns
        .iter()
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(
        schema.clone(),
        columns.into_iter().map(|(_, array)| array).collect(),
    )
    .unwrap();

    let engine = Engine::with_threads(1).unwrap();
    engine
        .register_batches("everything", &schema, std::slice::from_ref(&batch))
        .unwrap();
    let (result_schema, batches, end) = collect(engine.sql("SELECT * FROM everything").run(|| {}));
    end.unwrap();
    let result_schema = result_schema.unwrap();
    assert_eq!(result_schema.fields(), schema.fields());
    assert_eq!(concat(&result_schema, &batches), batch);
}

#[test]
fn an_empty_result_still_has_its_schema() {
    let engine = Engine::with_threads(1).unwrap();
    let (schema, batches) = readings();
    engine
        .register_batches("readings", &schema, &batches)
        .unwrap();
    let (schema, batches, end) = collect(
        engine
            .sql("SELECT sensor FROM readings WHERE false")
            .run(|| {}),
    );
    end.unwrap();
    assert_eq!(schema.unwrap().field(0).name(), "sensor");
    assert!(batches.is_empty());
}

#[test]
fn a_querys_columns_are_known_without_running_it() {
    let engine = Engine::with_threads(1).unwrap();
    let (schema, batches) = readings();
    engine
        .register_batches("readings", &schema, &batches)
        .unwrap();
    let columns = engine
        .schema("SELECT site, avg(celsius) AS celsius FROM readings GROUP BY site")
        .unwrap();
    let names: Vec<&str> = columns.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["site", "celsius"]);
    assert_eq!(columns.field(1).data_type(), &DataType::Float64);
    for (query, expected) in [
        ("SELECT * FROM nowhere", "nowhere"),
        ("DROP TABLE readings", "DDL"),
    ] {
        match engine.schema(query) {
            Err(EngineError::Query(reason)) => {
                assert!(reason.contains(expected), "{query}: {reason}")
            }
            other => panic!("{query}: {other:?}"),
        }
    }
    let message = engine.schema("SELEC").unwrap_err().to_string();
    assert!(message.starts_with("cannot plan the query: "), "{message}");
}

#[test]
fn queries_that_cannot_run_fail_with_a_reason() {
    let engine = Engine::with_threads(1).unwrap();
    for (query, expected) in [
        ("SELEC 1", "SELEC"),
        ("SELECT * FROM nowhere", "nowhere"),
        ("SELECT 1 / 0", "Divide by zero"),
    ] {
        let (_, _, end) = collect(engine.sql(query).run(|| {}));
        let reason = end.unwrap_err();
        assert!(reason.contains(expected), "{query}: {reason}");
    }
}

#[test]
fn tables_are_registered_once_by_name() {
    let engine = Engine::with_threads(1).unwrap();
    let (schema, batches) = readings();
    engine
        .register_batches("readings", &schema, &batches)
        .unwrap();
    let error = engine
        .register_batches("readings", &schema, &batches)
        .unwrap_err();
    assert!(matches!(error, EngineError::Register(_)), "{error}");
    assert!(engine.deregister("readings").unwrap());
    assert!(!engine.deregister("readings").unwrap());
    engine
        .register_batches("readings", &schema, &batches)
        .unwrap();
    let error = engine
        .register_parquet("missing", "no/such/file.parquet")
        .unwrap_err();
    assert!(
        error.to_string().starts_with("cannot register the table"),
        "{error}"
    );
}

#[test]
fn a_large_result_read_slowly_arrives_whole() {
    let engine = Engine::with_threads(2).unwrap();
    let mut stream = engine
        .sql("SELECT value FROM generate_series(1, 2000000)")
        .run(|| {});
    let mut rows = 0_u64;
    let mut sum = 0_u64;
    let mut taken = 0;
    loop {
        match stream.wait_next(WAIT) {
            Some(QueryEvent::Batch(batch)) => {
                rows += batch.num_rows() as u64;
                sum += batch
                    .column(0)
                    .as_primitive::<Int64Type>()
                    .values()
                    .iter()
                    .map(|&v| v as u64)
                    .sum::<u64>();
                taken += 1;
                if taken % 50 == 0 {
                    // A slow taker: the queue fills meanwhile.
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
            Some(QueryEvent::Finished) => break,
            Some(QueryEvent::Schema(_)) => {}
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(rows, 2_000_000);
    assert_eq!(sum, 2_000_000 * 2_000_001 / 2);
}

#[test]
fn dropping_a_query_cancels_it() {
    let engine = Engine::with_threads(1).unwrap();
    let held = Arc::new(());
    let in_query = held.clone();
    let stream = engine
        .sql("SELECT sum(value) FROM generate_series(1, 100000000000)")
        .run(move || {
            let _ = &in_query;
        });
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(Arc::strong_count(&held), 2, "running");
    drop(stream);
    let until = Instant::now() + WAIT;
    while Arc::strong_count(&held) > 1 && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        Arc::strong_count(&held),
        1,
        "the query and its waker are gone"
    );
}

#[test]
fn a_query_outlives_its_engine() {
    let engine = Engine::with_threads(1).unwrap();
    let stream = engine.sql("SELECT 41 + 1 AS answer").run(|| {});
    drop(engine);
    let (_, batches, end) = collect(stream);
    end.unwrap();
    assert_eq!(
        batches[0].column(0).as_primitive::<Int64Type>().value(0),
        42
    );
}

#[test]
fn row_counts_fit_the_unsigned_column_type() {
    // A u64 column keeps its type through the round trip.
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::UInt64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef],
    )
    .unwrap();
    let engine = Engine::with_threads(1).unwrap();
    engine.register_batches("big", &schema, &[batch]).unwrap();
    let (_, batches, end) = collect(engine.sql("SELECT n FROM big").run(|| {}));
    end.unwrap();
    assert_eq!(
        batches[0].column(0).as_primitive::<UInt64Type>().value(0),
        u64::MAX
    );
}

#[test]
fn a_column_of_nulls_alone_still_arrives() {
    let engine = Engine::with_threads(1).unwrap();
    let (schema, batches, end) = collect(engine.sql("SELECT NULL AS nothing").run(|| {}));
    end.unwrap();
    assert_eq!(schema.unwrap().field(0).data_type(), &DataType::Null);
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}

#[test]
fn row_numbers_key_a_result_that_has_none() {
    let engine = Engine::with_threads(2).unwrap();
    let query = engine
        .sql("SELECT value FROM generate_series(1, 20000)")
        .row_numbers("row");
    let (schema, batches, end) = collect(query.run(|| {}));
    end.unwrap();
    let schema = schema.unwrap();
    assert_eq!(schema.field(1).name(), "row");
    let mut numbers = Vec::new();
    for batch in &batches {
        assert_eq!(&batch.schema(), &schema);
        numbers.extend(
            batch
                .column(1)
                .as_primitive::<UInt64Type>()
                .values()
                .iter()
                .copied(),
        );
    }
    assert_eq!(numbers, (0..20_000).collect::<Vec<u64>>());

    let taken = engine.sql("SELECT 1 AS row").row_numbers("row");
    let (_, _, end) = collect(taken.run(|| {}));
    assert!(end.unwrap_err().contains("row"));
}

#[test]
fn queries_only_read_unless_changes_are_allowed() {
    let engine = Engine::with_threads(1).unwrap();
    let dir = std::env::temp_dir().join(format!("densa-datafusion-ro-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let copied = dir.join("copied.csv");
    let copy = format!("COPY (SELECT 1 AS a) TO '{}'", copied.display());
    for query in [
        copy.as_str(),
        "CREATE TABLE made AS SELECT 1 AS a",
        "SET datafusion.execution.batch_size = 1",
        "CREATE EXTERNAL TABLE outside STORED AS CSV LOCATION '.'",
    ] {
        let (_, _, end) = collect(engine.sql(query).run(|| {}));
        assert!(end.is_err(), "{query} ran");
    }
    assert!(!copied.exists(), "COPY wrote a file");

    let (_, _, end) = collect(
        engine
            .sql("CREATE TABLE made AS SELECT 7 AS a")
            .allow_changes()
            .run(|| {}),
    );
    end.unwrap();
    let (_, batches, end) = collect(engine.sql("SELECT a FROM made").run(|| {}));
    end.unwrap();
    assert_eq!(batches[0].column(0).as_primitive::<Int64Type>().value(0), 7);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_panic_while_querying_arrives_as_a_failure() {
    let engine = Engine::with_threads(1).unwrap();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = calls.clone();
    let stream = engine.sql("SELECT 1 AS one").run(move || {
        if counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            panic!("a waker that fails once");
        }
    });
    let (schema, _, end) = collect(stream);
    assert!(schema.is_some());
    assert!(end.unwrap_err().contains("panicked"));
}

#[test]
fn the_engine_works_from_inside_an_async_runtime() {
    let dir = std::env::temp_dir().join(format!("densa-datafusion-async-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let csv = dir.join("numbers.csv");
    std::fs::write(&csv, "n\n1\n2\n").unwrap();
    let app = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .unwrap();
    let rows = app.block_on(async {
        let engine = Engine::with_threads(1).unwrap();
        engine.register_csv("numbers", &csv).unwrap();
        let mut stream = engine.sql("SELECT sum(n) AS total FROM numbers").run(|| {});
        let mut total = None;
        while let Some(event) = stream.wait_next(WAIT) {
            match event {
                QueryEvent::Batch(batch) => {
                    total = Some(batch.column(0).as_primitive::<Int64Type>().value(0));
                }
                QueryEvent::Finished => break,
                _ => {}
            }
        }
        // Dropping both here, inside the runtime, must not panic.
        drop(stream);
        drop(engine);
        total
    });
    assert_eq!(rows, Some(3));
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_filtered_column_of_long_strings_crosses_without_its_neighbours() {
    // A Parquet string column is read as views; filtering keeps one row, whose views still
    // point into the buffers of every row.
    let dir = std::env::temp_dir().join(format!("densa-datafusion-views-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("notes.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("note", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (0..20_000).collect();
    let notes: Vec<String> = ids.iter().map(|i| format!("{i:0>200}")).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(notes)),
        ],
    )
    .unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let engine = Engine::with_threads(1).unwrap();
    engine.register_parquet("notes", &path).unwrap();
    let (schema, batches, end) = collect(
        engine
            .sql("SELECT note FROM notes WHERE id = 12345")
            .run(|| {}),
    );
    end.unwrap();
    let schema = schema.unwrap();
    let result = concat(&schema, &batches);
    assert_eq!(result.num_rows(), 1);
    assert_eq!(
        result.column(0).as_string_view_or_string(),
        [format!("{:0>200}", 12345)]
    );
    let held: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
    assert!(held < 200_000, "one row holds {held} bytes");

    // A limit slices the batch it stops in, its views pointing into every row's bytes.
    let (schema, batches, end) = collect(
        engine
            .sql("SELECT note FROM notes LIMIT 1 OFFSET 3000")
            .run(|| {}),
    );
    end.unwrap();
    let result = concat(&schema.unwrap(), &batches);
    assert_eq!(
        result.column(0).as_string_view_or_string(),
        [format!("{:0>200}", 3000)]
    );
    let held: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
    assert!(held < 200_000, "one row of a limit holds {held} bytes");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn types_the_engine_makes_cross_unchanged() {
    let engine = Engine::with_threads(1).unwrap();
    let query = "SELECT arrow_cast('text', 'Utf8View') AS view, \
                 arrow_cast('bytes', 'BinaryView') AS binary_view, \
                 arrow_cast(5, 'Duration(Second)') AS duration, \
                 INTERVAL '1 day' AS interval, \
                 TIME '12:30:00' AS time, \
                 named_struct('x', 1, 'y', 'two') AS point";
    let (schema, batches, end) = collect(engine.sql(query).run(|| {}));
    end.unwrap();
    let schema = schema.unwrap();
    let kinds: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect();
    assert_eq!(kinds[0], DataType::Utf8View);
    assert_eq!(kinds[1], DataType::BinaryView);
    assert!(matches!(kinds[2], DataType::Duration(_)), "{:?}", kinds[2]);
    assert!(matches!(kinds[3], DataType::Interval(_)), "{:?}", kinds[3]);
    assert!(
        matches!(kinds[4], DataType::Time64(_) | DataType::Time32(_)),
        "{:?}",
        kinds[4]
    );
    assert!(matches!(kinds[5], DataType::Struct(_)), "{:?}", kinds[5]);
    let result = concat(&schema, &batches);
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.column(0).as_string_view().value(0), "text");
    assert_eq!(result.column(1).as_binary_view().value(0), b"bytes");
}

#[test]
fn a_failure_after_some_rows_ends_the_result() {
    let engine = Engine::with_threads(1).unwrap();
    // The first batch is fine; dividing by zero comes in the second.
    let query = "SELECT 10 / (value - 9000) AS x FROM generate_series(1, 20000)";
    let (schema, batches, end) = collect(engine.sql(query).run(|| {}));
    assert!(schema.is_some());
    assert!(!batches.is_empty(), "rows before the failure");
    assert!(end.unwrap_err().contains("Divide by zero"));
}

#[test]
fn queries_run_side_by_side() {
    let engine = Engine::with_threads(2).unwrap();
    let first = engine
        .sql("SELECT count(*) AS n FROM generate_series(1, 300000)")
        .run(|| {});
    let second = engine
        .sql("SELECT count(*) AS n FROM generate_series(1, 200000)")
        .run(|| {});
    let count = |stream| {
        let (_, batches, end) = collect(stream);
        end.unwrap();
        batches[0].column(0).as_primitive::<Int64Type>().value(0)
    };
    assert_eq!(count(second), 200_000);
    assert_eq!(count(first), 300_000);
}

#[test]
fn batches_of_another_schema_are_refused() {
    let engine = Engine::with_threads(1).unwrap();
    let (schema, batches) = readings();
    let other = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let error = engine
        .register_batches("readings", &other, &batches)
        .unwrap_err();
    assert!(error.to_string().contains("differ"), "{error}");
    engine
        .register_batches("readings", &schema, &batches)
        .unwrap();
}

#[test]
fn names_are_taken_as_they_are() {
    let engine = Engine::with_threads(1).unwrap();
    let (schema, batches) = readings();
    engine
        .register_batches("Plant.readings", &schema, &batches)
        .unwrap();
    let (_, batches, end) = collect(
        engine
            .sql(r#"SELECT count(*) AS n FROM "Plant.readings""#)
            .run(|| {}),
    );
    end.unwrap();
    assert_eq!(batches[0].column(0).as_primitive::<Int64Type>().value(0), 5);
    assert!(engine.deregister("Plant.readings").unwrap());

    let dir = std::env::temp_dir().join(format!("densa-datafusion-names-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let csv = dir.join("sites.csv");
    std::fs::write(
        &csv,
        "site
north
south
",
    )
    .unwrap();
    engine.register_csv("Plant.sites", &csv).unwrap();
    let (_, batches, end) = collect(
        engine
            .sql(r#"SELECT count(*) AS n FROM "Plant.sites""#)
            .run(|| {}),
    );
    end.unwrap();
    assert_eq!(batches[0].column(0).as_primitive::<Int64Type>().value(0), 2);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn empty_batches_are_not_handed_over() {
    let engine = Engine::with_threads(1).unwrap();
    let (schema, batches) = readings();
    let empty = RecordBatch::new_empty(schema.clone());
    let with_empty = [empty.clone(), batches[0].clone(), empty];
    engine
        .register_batches("gappy", &schema, &with_empty)
        .unwrap();
    let (_, batches, end) = collect(engine.sql("SELECT * FROM gappy").run(|| {}));
    end.unwrap();
    assert!(
        batches.iter().all(|b| b.num_rows() > 0),
        "{:?}",
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>()
    );
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
}

/// `std::fs::canonicalize` gives a verbatim path on Windows (`\\?\C:\...`), which DataFusion
/// cannot make a URL of: the engine takes it all the same.
#[cfg(windows)]
#[test]
fn a_file_named_by_its_canonical_path_is_registered() {
    let path =
        std::env::temp_dir().join(format!("densa-df-{}-canonical.parquet", std::process::id()));
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..10));
    let batch = RecordBatch::try_from_iter([("id", ids)]).unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None)
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let canonical = std::fs::canonicalize(&path).unwrap();
    assert!(
        canonical.to_string_lossy().starts_with(r"\\?\"),
        "{}",
        canonical.display()
    );
    let engine = Engine::with_threads(1).unwrap();
    let registered = engine.register_parquet("canonical", &canonical);
    let (_, batches, end) = collect(engine.sql("SELECT count(*) FROM canonical").run(|| {}));
    let _ = std::fs::remove_file(&path);
    registered.unwrap();
    end.unwrap();
    assert_eq!(
        batches[0].column(0).as_primitive::<Int64Type>().value(0),
        10
    );
}

/// Writes `ids` 0, 1, ... as a Parquet file at `path`.
fn write_ids(path: &std::path::Path, rows: i64) {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows));
    let batch = RecordBatch::try_from_iter([("id", ids)]).unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), None)
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// The count `query` gives.
fn count(engine: &Engine, query: &str) -> i64 {
    let (_, batches, end) = collect(engine.sql(query).run(|| {}));
    end.unwrap();
    batches[0].column(0).as_primitive::<Int64Type>().value(0)
}

#[test]
fn a_parquet_file_is_registered_whatever_its_name_ends_in_or_holds() {
    let folder = std::env::temp_dir().join(format!("densa-df-{}-names", std::process::id()));
    std::fs::create_dir_all(&folder).unwrap();
    // `[1]` would be a pattern to DataFusion, matching `1` (Windows allows no `*` or `?`).
    let names = [
        "a.pq",
        "b.parq",
        "C.PARQUET",
        "no extension",
        "d [1].parquet",
    ];
    let engine = Engine::with_threads(1).unwrap();
    for (at, name) in names.iter().enumerate() {
        write_ids(&folder.join(name), 10 + at as i64);
        let table = format!("t{at}");
        let registered = engine.register_parquet(&table, folder.join(name));
        assert!(registered.is_ok(), "{name}: {registered:?}");
        let rows = count(&engine, &format!("SELECT count(*) FROM {table}"));
        assert_eq!(rows, 10 + at as i64, "{name}");
    }
    // A folder's files are those named .parquet: of these, only the last.
    engine.register_parquet("folder", &folder).unwrap();
    assert_eq!(count(&engine, "SELECT count(*) FROM folder"), 14);
    let _ = std::fs::remove_dir_all(&folder);
}

#[test]
fn a_memory_limit_keeps_the_tables_and_says_so_when_a_query_needs_more() {
    let (schema, batches) = readings();
    let engine = Engine::with_threads(2).unwrap();
    engine
        .register_batches("readings", &schema, &batches)
        .unwrap();
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..200_000));
    let wide = RecordBatch::try_from_iter([("id", ids)]).unwrap();
    engine
        .register_batches("wide", &wide.schema(), std::slice::from_ref(&wide))
        .unwrap();
    let engine = engine.with_memory_limit(1 << 20).unwrap();
    // Registered before: still there, and a small query runs.
    let (_, rows, end) = collect(engine.sql("SELECT * FROM readings").run(|| {}));
    end.unwrap();
    assert!(!rows.is_empty());
    // A join's table cannot spill: 200,000 ids hold more than a megabyte.
    let joined = "SELECT count(*) FROM wide a JOIN wide b ON a.id = b.id";
    let (_, _, end) = collect(engine.sql(joined).run(|| {}));
    let reason = end.unwrap_err();
    assert!(
        reason.contains("1048576 bytes of memory the engine allows")
            && reason.contains("Engine::with_memory_limit"),
        "{reason}"
    );
}

#[test]
fn a_sort_larger_than_the_memory_limit_spills_and_finishes() {
    // 4 million ids, 32 MB, sorted with 16 MiB allowed.
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..4_000_000));
    let ids = RecordBatch::try_from_iter([("id", ids)]).unwrap();
    let batches: Vec<RecordBatch> = (0..ids.num_rows())
        .step_by(65_536)
        .map(|at| ids.slice(at, 65_536.min(ids.num_rows() - at)))
        .collect();
    let engine = Engine::with_threads(2)
        .unwrap()
        .with_memory_limit(16 << 20)
        .unwrap();
    engine
        .register_batches("ids", &ids.schema(), &batches)
        .unwrap();
    let sorted = "SELECT id FROM ids ORDER BY id DESC";
    let (_, batches, end) = collect(engine.sql(sorted).run(|| {}));
    end.unwrap();
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 4_000_000);
    let first = batches[0].column(0).as_primitive::<Int64Type>().value(0);
    assert_eq!(first, 3_999_999);
}
