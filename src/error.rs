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
    Corruption {
        offset: u64,
        reason: String,
    },
    BatchTooLarge {
        points: usize,
        maximum: usize,
    },
    InvalidConfig(&'static str),
    /// A caller-supplied argument violates a documented API precondition,
    /// such as a non-positive rollup resolution or out-of-order aggregate
    /// samples. Unlike `InvalidConfig`, which rejects a durable handle or
    /// policy setting, this rejects one runtime call.
    InvalidArgument(&'static str),
    InvalidModel(String),
    Serialization(String),
    Poisoned,
    Locked {
        path: PathBuf,
    },
    ReadOnly,
    /// A no-clobber rename succeeded, but a later durability or verification
    /// step failed and the process could not fully roll its own directory back.
    SnapshotPublication {
        path: PathBuf,
        reason: String,
    },
    SourceChanged {
        path: PathBuf,
    },
    /// A producer reused one source sequence with a different transaction or
    /// commit identifier. The writer remains usable.
    IngressSourceSequenceConflict {
        source_id: u128,
        sequence: u64,
    },
    /// A producer reused one commit identifier for another source, sequence,
    /// or transaction payload. The writer remains usable.
    IngressCommitIdConflict {
        commit_id: u128,
    },
    /// A producer supplied a new cursor that did not advance. Gaps are valid:
    /// the sequence is an opaque source cursor, not a dense counter.
    IngressSequenceNotIncreasing {
        source_id: u128,
        previous: u64,
        actual: u64,
    },
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
            Self::InvalidArgument(reason) => write!(f, "invalid argument: {reason}"),
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
            Self::SnapshotPublication { path, reason } => write!(
                f,
                "snapshot publication at {} could not be rolled back cleanly: {reason}",
                path.display()
            ),
            Self::SourceChanged { path } => write!(
                f,
                "source file {} changed while it was being checked",
                path.display()
            ),
            Self::IngressSourceSequenceConflict {
                source_id,
                sequence,
            } => write!(
                f,
                "ingress source {source_id:032x} sequence {sequence} conflicts with its stored transaction"
            ),
            Self::IngressCommitIdConflict { commit_id } => write!(
                f,
                "commit identifier {commit_id:032x} conflicts with its stored transaction"
            ),
            Self::IngressSequenceNotIncreasing {
                source_id,
                previous,
                actual,
            } => write!(
                f,
                "ingress source {source_id:032x} requires a cursor above {previous}, got {actual}"
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
