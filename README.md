# densa connectors

Data sources for [densa](https://github.com/densa-rs/densa), the Rust toolkit for
applications that show a lot of live data. Each connector turns a source into Apache Arrow
record batches, the form densa's live tables, grid and charts take, without depending on
densa itself: any Arrow consumer can use them.

Connectors live apart from densa's core crates because the formats and protocols they speak
bring large dependency trees of their own.

> **Status: 0.x, experimental.** Not on crates.io yet; densa's crates are taken from their
> repository.

## Crates

| Crate | Use it for |
|---|---|
| [`densa-files`](crates/densa-files) | CSV and Parquet files as record batches: CSV column types inferred, row numbers for a key, read on a background thread that wakes the interface as each batch is ready |
| [`densa-ws`](crates/densa-ws) | WebSocket streams of JSON rows or Arrow IPC as record batches: rows gathered into batches, handed over within a set delay, reconnecting with growing waits and re-sending subscriptions, TLS through rustls |
| [`densa-flight`](crates/densa-flight) | Arrow Flight and Flight SQL results as record batches: tickets, dataset paths, commands and SQL queries, every endpoint read in order, row numbers for a key, optional retries for streams that never end; no async runtime needed in the app |

## A file in a live table

```rust
use std::error::Error;

use densa_data::LiveTable;
use densa_files::{Loader, OpenOptions};

fn load(path: &str) -> Result<LiveTable, Box<dyn Error>> {
    // Rows numbered as they are read, so the file needs no key column of its own.
    let reader = OpenOptions::new().row_numbers("row").open(path)?;
    let mut table = LiveTable::new(reader.schema(), "row")?;
    let mut loader = Loader::spawn(reader, || { /* waker.wake() */ })?;

    // In an app, take what is ready each frame (`try_next`) so the grid grows as it reads.
    while let Some(batch) = loader.wait_next() {
        table.upsert(batch?)?;
    }
    Ok(table)
}
```

CSV column types are inferred from the first records; `OpenOptions::infer_rows` reads more
of them, and `OpenOptions::as_text` reads every column as text when a file's types change
partway through.

## A stream in a live table

```rust
use std::sync::Arc;
use std::time::Duration;

use arrow_schema::{DataType, Field, Schema};
use densa_data::LiveTable;
use densa_ws::{StreamEvent, StreamOptions};

fn follow() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("sensor", DataType::Utf8, false),
        Field::new("celsius", DataType::Float64, true),
    ]));
    let mut table = LiveTable::new(schema.clone(), "sensor")?;
    let mut stream = StreamOptions::json("wss://example.com/readings", schema)
        .on_connect(r#"{"subscribe":"readings"}"#)
        .max_delay(Duration::from_millis(50))
        .connect(|| { /* waker.wake() */ })?;

    // In an app, take what is ready each frame with `try_next`.
    while let Some(event) = stream.wait_next(Duration::from_secs(1)) {
        match event {
            StreamEvent::Batch(batch) => {
                table.upsert(batch)?;
            }
            StreamEvent::Disconnected { reason, retry_in } => {
                eprintln!("{reason}; trying again in {retry_in:?}");
            }
            _ => {}
        }
    }
    Ok(())
}
```

A JSON message holds one or more objects; one that cannot be read is skipped whole and
reported. Arrow IPC streams may be split across binary messages in any way. A lost
connection is opened again, and the subscriptions sent again, after a wait that doubles up
to a limit.

## A Flight SQL query in a live table

```rust
use densa_data::LiveTable;
use densa_flight::{FlightEvent, FlightOptions};

fn query() -> Result<(), Box<dyn std::error::Error>> {
    let mut result = FlightOptions::sql("grpc+tls://flight.example.com:443", "SELECT * FROM trips")
        .header("authorization", "Bearer <token>")
        .row_numbers("row")
        .connect(|| { /* waker.wake() */ })?;

    // In an app, take what is ready each frame with `try_next`.
    let mut table = None;
    while let Some(event) = result.wait_next(std::time::Duration::from_secs(30)) {
        match event {
            FlightEvent::Schema(schema) => table = Some(LiveTable::new(schema, "row")?),
            FlightEvent::Batch(batch) => {
                if let Some(table) = table.as_mut() {
                    table.upsert(batch)?;
                }
            }
            FlightEvent::Failed { reason } => return Err(reason.into()),
            _ => {}
        }
    }
    Ok(())
}
```

The schema comes first, so the table exists even for an empty result. `FlightOptions::ticket`,
`path` and `command` ask for other kinds of data; a `DoGet` that never ends (live data) is
read as it comes. A failure ends the result unless `FlightOptions::retry` is set (unlike
WebSocket streams, which always reconnect), and failures that would repeat, such as a refused
query, always end it. Headers go only to the service named by the URL, never to endpoints it
places elsewhere.

## Applications

`file-viewer` opens a CSV or Parquet file in densa's live grid while the file loads: rows
appear as they are read, and a click on a title sorts them.

```
cargo run --release -p file-viewer -- requests.parquet        # keyed by row number
cargo run --release -p file-viewer -- sensors.csv --key id    # keyed by a column
cargo run --release -p file-viewer -- --sample                # a million sample requests
```

<p align="center">
  <img alt="A Parquet file of a million web requests in densa's grid: request, time, service, method, path, status, duration and size columns" src="https://raw.githubusercontent.com/densa-rs/densa-connectors/main/docs/file-viewer.png" width="660">
</p>

`stream-viewer` shows a WebSocket stream in the grid, the latest row of every key, with the
connection's state and the rate in the status bar.

```
cargo run --release -p stream-viewer -- --demo     # a local server of 500 sensors
cargo run --release -p stream-viewer -- wss://example.com/readings \
    --columns sensor:string,celsius:float,time:time --key sensor \
    --subscribe '{"subscribe":"readings"}'
```

Column types are `string`, `int`, `float`, `bool` and `time` (milliseconds since 1970).

<p align="center">
  <img alt="A WebSocket stream of 500 sensors in densa's grid: sensor, site, celsius, humidity, ok and time columns, with the connection and rows a second in the status bar" src="https://raw.githubusercontent.com/densa-rs/densa-connectors/main/docs/stream-viewer.png" width="660">
</p>

`flight-viewer` shows a Flight or Flight SQL result in the grid as it arrives.

```
cargo run --release -p flight-viewer -- --demo     # a local service: a million requests
cargo run --release -p flight-viewer -- grpc://localhost:32010 --sql "SELECT * FROM trips" \
    --header 'authorization:$FLIGHT_AUTH'          # FLIGHT_AUTH="Bearer <token>"
```

`--ticket`, `--path a/b` and `--command` ask for other kinds of data; `--key COLUMN` keys rows
by a column instead of numbering them.

<p align="center">
  <img alt="A Flight SQL result of a million web requests in densa's grid: request, time, service, method, path, status, ms, bytes and row number columns, with the row count in the status bar" src="https://raw.githubusercontent.com/densa-rs/densa-connectors/main/docs/flight-viewer.png" width="660">
</p>

## Next

Query engines that already speak Arrow (DataFusion, Polars, DuckDB), then Kafka and NATS.
The roadmap is at [densa.dev/roadmap](https://densa.dev/roadmap/).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you explicitly state otherwise, any
contribution intentionally submitted for inclusion in the work by you, as defined in the
Apache-2.0 license, shall be dual licensed as above, without any additional terms or
conditions.
