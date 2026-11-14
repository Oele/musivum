//! The errors of `hwstats`, written new: the original returns the errors of the system, of
//! `strconv` and of `go-osstat` as they come, and two of its own made with `errors.New`.

use std::io;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// An error of the system, with the text Rust gives it; `kind` is what Go's
    /// `errors.Is(err, os.ErrNotExist)` asks.
    #[error("{text}")]
    Os { kind: io::ErrorKind, text: String },
    /// A number that does not parse, with the text Rust gives it.
    #[error("{0}")]
    Parse(String),
    /// What `errors.New` and `fmt.Errorf` make, with the original's text.
    #[error("{0}")]
    Other(String),
}

impl Error {
    pub fn is_not_exist(&self) -> bool {
        matches!(self, Error::Os { kind, .. } if *kind == io::ErrorKind::NotFound)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Os {
            kind: err.kind(),
            text: err.to_string(),
        }
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Error {
        Error::Parse(err.to_string())
    }
}
