use std::fmt;
use std::path::PathBuf;

use crate::FileFormat;

/// Why a file could not be opened or read.
#[derive(Debug)]
#[non_exhaustive]
pub enum FileError {
    /// The file could not be opened or read from.
    Io(std::io::Error),
    /// The format cannot be told from this path's extension: give one with
    /// [`OpenOptions::format`](crate::OpenOptions::format).
    UnknownFormat(PathBuf),
    /// The file's content is not valid in its format (the reader's message).
    Invalid(String),
    /// A column named for row numbers is already in the file.
    DuplicateColumn(String),
    /// The path is a folder, not a file.
    Folder(PathBuf),
    /// Files of this format cannot be read a page at a time (`OpenOptions::paged`, feature
    /// `paged`): CSV has no index of its rows.
    NotPageable(FileFormat),
    /// The file was written to since its rows were counted (`FilePages`, feature `paged`):
    /// count them again.
    Changed(PathBuf),
    /// The file's columns are not the ones it had when opened (`FilePages`, feature
    /// `paged`): open it again.
    ColumnsChanged(PathBuf),
}

impl fmt::Display for FileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "could not read the file: {error}"),
            Self::UnknownFormat(path) => {
                write!(f, "cannot tell the format of {}", path.display())
            }
            Self::Invalid(message) => write!(f, "invalid file: {message}"),
            Self::DuplicateColumn(name) => {
                write!(f, "the file already has a column named {name:?}")
            }
            Self::Folder(path) => write!(
                f,
                "{} is a folder: open one of its files, or query the folder with DataFusion \
                 (densa-datafusion)",
                path.display()
            ),
            Self::NotPageable(format) => write!(
                f,
                "a {format} file cannot be read a page at a time, as it has no index of its \
                 rows: load it whole (Loader), or query it with DataFusion (densa-datafusion)"
            ),
            Self::Changed(path) => write!(
                f,
                "{} was written to since its rows were counted: count them again (refresh the \
                 model, or FilePages::reload)",
                path.display()
            ),
            Self::ColumnsChanged(path) => write!(
                f,
                "the columns of {} changed since it was opened: open it again",
                path.display()
            ),
        }
    }
}

impl std::error::Error for FileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for FileError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// An Arrow, CSV or Parquet error as a [`FileError`], without their types in the API.
pub(crate) fn invalid(error: impl fmt::Display) -> FileError {
    FileError::Invalid(error.to_string())
}
