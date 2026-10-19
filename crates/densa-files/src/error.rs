use std::fmt;
use std::path::PathBuf;

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
