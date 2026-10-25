use std::error::Error;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::{requests, REQUESTS};

/// Rows the requests are written in at a time, and in a row group: eight groups, so a page
/// deep in the file comes from a group of its own.
const BATCH: i64 = 65_536;
const GROUP_ROWS: usize = 131_072;

/// The number of the next sample file this process writes, so each has a name of its own.
static NEXT: AtomicU32 = AtomicU32::new(0);

/// A sample file in the temporary folder, removed when dropped: keep it while it is shown.
#[derive(Debug)]
#[must_use = "the file is removed when this is dropped"]
pub struct SampleFile(PathBuf);

impl SampleFile {
    /// Where the file is.
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for SampleFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

/// Writes the [`REQUESTS`] to a new Parquet file in the temporary folder (snappy, in row groups
/// of 131,072 rows).
///
/// # Errors
/// The file cannot be written, or one of its name is already there.
pub fn requests_file() -> Result<SampleFile, Box<dyn Error>> {
    // Relaxed: only that no two calls get one number matters.
    let path = sample_path(NEXT.fetch_add(1, Ordering::Relaxed));
    // A new file, never one already there (which is not ours to remove).
    let created = File::options().write(true).create_new(true).open(&path)?;
    let file = SampleFile(path);
    let first = requests(0, BATCH)?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(GROUP_ROWS))
        .build();
    let mut writer = ArrowWriter::try_new(created, first.schema(), Some(properties))?;
    writer.write(&first)?;
    let mut start = BATCH;
    while start < REQUESTS {
        writer.write(&requests(start, (start + BATCH).min(REQUESTS))?)?;
        start += BATCH;
    }
    writer.close()?;
    Ok(file)
}

/// Where this process's sample file `number` goes.
fn sample_path(number: u32) -> PathBuf {
    let name = format!(
        "densa-sample-requests-{}-{number}.parquet",
        std::process::id()
    );
    std::env::temp_dir().join(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_sample_file_is_new_and_goes_with_its_guard() {
        let first = requests_file().unwrap();
        let second = requests_file().unwrap();
        assert_ne!(first.path(), second.path());
        let path = first.path().to_owned();
        drop(first);
        assert!(!path.exists() && second.path().exists());
        // A file already where the next one would go is neither written over nor removed.
        // Relaxed: this test alone writes sample files.
        let taken = sample_path(NEXT.load(Ordering::Relaxed));
        fs::write(&taken, "someone's").unwrap();
        assert!(requests_file().is_err());
        let left = fs::read_to_string(&taken);
        let _ = fs::remove_file(&taken);
        assert_eq!(left.unwrap(), "someone's");
    }
}
