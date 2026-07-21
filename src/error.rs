use std::error::Error as StdError;
use std::fmt;
use std::io;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    InvalidHeader,
    UnsupportedVersion(u16),
    Corruption { offset: u64, reason: String },
    BatchTooLarge { points: usize, maximum: usize },
    InvalidConfig(&'static str),
    Poisoned,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::InvalidHeader => write!(f, "invalid WattDB file header"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported WattDB format version {version}")
            }
            Self::Corruption { offset, reason } => {
                write!(f, "corruption at file offset {offset}: {reason}")
            }
            Self::BatchTooLarge { points, maximum } => {
                write!(f, "batch has {points} points; maximum is {maximum}")
            }
            Self::InvalidConfig(reason) => write!(f, "invalid configuration: {reason}"),
            Self::Poisoned => write!(
                f,
                "database writer is poisoned after a failed write; close and reopen it"
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
