#![warn(missing_docs)]
#![feature(async_iterator)]

//! A library for iterating over a MongoDB replica set oplog.
//!
//! Given a MongoDB `Client` connected to a replica set, this crate allows you to iterate over an
//! `Oplog` as if it were a collection of statically typed `Operation`s.
//!
//! # Example
//!
//! At its most basic, an `Oplog` will yield _all_ operations in the oplog when iterated over:
//!
//! ```rust,no_run
//! # extern crate mongodb;
//! # extern crate oplog;
//! use mongodb::{Client, ThreadedClient};
//! use oplog::Oplog;
//!
//! # fn main() {
//! let client = Client::connect("localhost", 27017).expect("Failed to connect to MongoDB.");
//!
//! if let Ok(oplog) = Oplog::new(&client) {
//!     for operation in oplog {
//!         // Do something with operation...
//!     }
//! }
//! # }
//! ```
//!
//! Alternatively, an `Oplog` can be built with a filter via `OplogBuilder` to restrict the
//! operations yielded:
//!
//! ```rust,no_run
//! # #[macro_use]
//! # extern crate bson;
//! # extern crate mongodb;
//! # extern crate oplog;
//! use mongodb::{Client, ThreadedClient};
//! use oplog::OplogBuilder;
//!
//! # fn main() {
//! let client = Client::connect("localhost", 27017).expect("Failed to connect to MongoDB.");
//!
//! if let Ok(oplog) = OplogBuilder::new(&client).filter(Some(doc! { "op" => "i" })).build() {
//!     for insert in oplog {
//!         // Do something with insert operation...
//!     }
//! }
//! # }
//! ```

use std::error;
use std::fmt;
use std::result;

use mongodb::bson::document::ValueAccessError;
pub use operation::Operation;
// pub use oplog::{Oplog, OplogBuilder};

mod operation;
mod oplog;

/// A type alias for convenience so we can fix the error to our own `Error` type.
pub type Result<T> = result::Result<T, Error>;

/// Error enumerates the list of possible error conditions when tailing an oplog.
#[derive(Debug)]
pub enum Error {
    /// A database connectivity error raised by the MongoDB driver.
    Database(mongodb::error::Error),
    /// An error when converting a BSON document to an `Operation` and it has a missing field or
    /// unexpected type.
    MissingField(ValueAccessError),
    /// An error when converting a BSON document to an `Operation` and it has an unsupported
    /// operation type.
    UnknownOperation(String),
    /// An error when converting an applyOps command with invalid documents.
    InvalidOperation,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Database(ref err) => err.fmt(f),
            Error::MissingField(ref err) => err.fmt(f),
            Error::UnknownOperation(ref op) => write!(f, "Unknown operation type found: {}", op),
            Error::InvalidOperation => write!(f, "Invalid operation"),
        }
    }
}

impl From<mongodb::error::Error> for Error {
    fn from(original: mongodb::error::Error) -> Error {
        Error::Database(original)
    }
}
