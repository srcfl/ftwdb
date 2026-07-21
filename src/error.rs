use std::error::Error as StdError;
use std::fmt;
use std::io;
use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    InvalidHeader,
    UnsupportedVersion(u16),
    Corruption { offset: u64, reason: String },
    BatchTooLarge { points: usize, maximum: usize },
    InvalidConfig(&'static str),
    InvalidModel(String),
    Serialization(String),
    Poisoned,
    Locked { path: PathBuf },
    ReadOnly,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::InvalidHeader => write!(f, "invalid FTWDB file header"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported FTWDB format version {version}")
            }
            Self::Corruption { offset, reason } => {
                write!(f, "corruption at file offset {offset}: {reason}")
            }
            Self::BatchTooLarge { points, maximum } => {
                write!(f, "batch has {points} points; maximum is {maximum}")
            }
            Self::InvalidConfig(reason) => write!(f, "invalid configuration: {reason}"),
            Self::InvalidModel(reason) => write!(f, "invalid energy model: {reason}"),
            Self::Serialization(reason) => write!(f, "serialization error: {reason}"),
            Self::Poisoned => write!(
                f,
                "database writer is poisoned after a failed write; close and reopen it"
            ),
            Self::Locked { path } => write!(
                f,
                "database file {} is locked by another process; close the other opener first",
                path.display()
            ),
            Self::ReadOnly => write!(
                f,
                "database is open read-only; reopen it writable to modify it"
            ),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
