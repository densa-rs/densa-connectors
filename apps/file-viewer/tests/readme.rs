//! The README's sample, compiled and run, so it stays true.

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

#[test]
fn the_readme_sample_loads_a_file_into_a_live_table() {
    let path = std::env::temp_dir().join(format!("densa-readme-{}.csv", std::process::id()));
    std::fs::write(&path, "symbol,price\nA,1.5\nB,2.5\nA,3.5\n").unwrap();
    let table = load(path.to_str().unwrap()).unwrap();
    let _ = std::fs::remove_file(&path);
    assert_eq!(table.len(), 3, "numbered rows: no two share a key");
}
