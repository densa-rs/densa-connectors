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

## Applications

`file-viewer` opens a CSV or Parquet file in densa's live grid while the file loads: rows
appear as they are read, and a click on a title sorts them.

```
cargo run --release -p file-viewer -- orders.parquet          # keyed by row number
cargo run --release -p file-viewer -- trades.csv --key id     # keyed by a column
cargo run --release -p file-viewer -- --sample                # a million sample orders
```

<p align="center">
  <img alt="A Parquet file of a million orders in densa's grid: order id, time, symbol, side, price and quantity columns" src="https://raw.githubusercontent.com/densa-rs/densa-connectors/main/docs/file-viewer.png" width="660">
</p>

## Next

WebSocket streams, and Arrow Flight. The roadmap is at [densa.dev/vision](https://densa.dev/vision).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you explicitly state otherwise, any
contribution intentionally submitted for inclusion in the work by you, as defined in the
Apache-2.0 license, shall be dual licensed as above, without any additional terms or
conditions.
