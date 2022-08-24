//! The operation module is responsible for converting MongoDB BSON documents into specific
//! `Operation` types, one for each type of document stored in the MongoDB oplog. As much as
//! possible, we convert BSON types into more typical Rust types (e.g. BSON timestamps into UTC
//! datetimes).
//!
//! As we accept _any_ document, it may not be a valid operation so wrap any conversions in a
//! `Result`.

use std::fmt;

use crate::Result;
use mongodb::bson::{Bson, DateTime, Document};

/// A MongoDB oplog operation.
#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
pub enum Operation {
    /// An insert of a document into a specific database and collection.
    Insert {
        /// The time of the operation.
        timestamp: DateTime,
        /// The full namespace of the operation including its database and collection.
        namespace: String,
        /// The BSON document inserted into the namespace.
        document: Document,
    },
    /// An update of a document in a specific database and collection matching a given query.
    Update {
        /// The time of the operation.
        timestamp: DateTime,
        /// The full namespace of the operation including its database and collection.
        namespace: String,
        // /// The BSON selection criteria for the update.
        // query: Document,
        /// The BSON update applied in this operation.
        document: Document,
    },
    /// The deletion of a document in a specific database and collection matching a given query.
    Delete {
        /// The time of the operation.
        timestamp: DateTime,
        /// The full namespace of the operation including its database and collection.
        namespace: String,
        /// The BSON selection criteria for the delete.
        document: Document,
    },
}

impl Operation {
    pub fn new(document: &Document) -> Result<Operation> {
        let op = document
            .get_str("op")
            .map_err(|e| crate::Error::MissingField(e))?;

        match op {
            "i" => Operation::from_insert(document),
            "u" => Operation::from_update(document),
            "d" => Operation::from_delete(document),
            op => Err(crate::Error::UnknownOperation(op.into())),
        }
    }

    /// Returns an operation from any BSON value.
    fn from_bson(bson: &Bson) -> Result<Operation> {
        match *bson {
            Bson::Document(ref document) => Operation::new(document),
            _ => Err(crate::Error::InvalidOperation),
        }
    }

    /// Return an insert operation for a given document.
    fn from_insert(document: &Document) -> Result<Operation> {
        let ts = document
            .get_timestamp("ts")
            .map_err(|e| crate::Error::MissingField(e))?;
        let ns = document
            .get_str("ns")
            .map_err(|e| crate::Error::MissingField(e))?;
        let o = document
            .get_document("o")
            .map_err(|e| crate::Error::MissingField(e))?;

        Ok(Operation::Insert {
            timestamp: DateTime::from_millis((ts.time + ts.increment) as i64),
            namespace: ns.into(),
            document: o.to_owned(),
        })
    }

    /// Return an update operation for a given document.
    fn from_update(document: &Document) -> Result<Operation> {
        let ts = document
            .get_timestamp("ts")
            .map_err(|e| crate::Error::MissingField(e))?;
        let ns = document
            .get_str("ns")
            .map_err(|e| crate::Error::MissingField(e))?;
        let o = document
            .get_document("o")
            .map_err(|e| crate::Error::MissingField(e))?;

        Ok(Operation::Update {
            timestamp: DateTime::from_millis((ts.time + ts.increment) as i64),
            namespace: ns.into(),
            document: o.to_owned(),
        })
    }

    /// Return a delete operation for a given document.
    fn from_delete(document: &Document) -> Result<Operation> {
        let ts = document
            .get_timestamp("ts")
            .map_err(|e| crate::Error::MissingField(e))?;
        let ns = document
            .get_str("ns")
            .map_err(|e| crate::Error::MissingField(e))?;
        let o = document
            .get_document("o")
            .map_err(|e| crate::Error::MissingField(e))?;

        Ok(Operation::Delete {
            timestamp: DateTime::from_millis((ts.time + ts.increment) as i64),
            namespace: ns.into(),
            document: o.to_owned(),
        })
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &*self {
            Operation::Insert {
                timestamp,
                ref namespace,
                ref document,
            } => {
                write!(
                    f,
                    "Insert # into {} at {}: {}",
                    namespace, timestamp, document
                )
            }
            Operation::Update {
                timestamp,
                ref namespace,
                ref document,
            } => {
                write!(
                    f,
                    "Update #{} at {}: {}",
                    namespace, timestamp, document
                )
            }
            Operation::Delete {
                timestamp,
                ref namespace,
                ref document,
            } => {
                write!(f, "Delete # from {} at {}: {}", namespace, timestamp, document)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Operation;
    use crate::Error;
    use mongodb::bson::{doc, document::ValueAccessError, DateTime, Timestamp};

    #[test]
    fn operation_converts_inserts() {
        let doc = doc! {
            "ts":  Timestamp{time:1479419534,increment:0},
            "op": "i",
            "ns": "foo.bar",
            "o": {
                "foo": "bar"
            }
        };
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::Insert {
                timestamp: DateTime::from_millis(1479419534),
                namespace: "foo.bar".into(),
                document: doc! { "foo" : "bar" },
            }
        );
    }

    #[test]
    fn operation_converts_updates() {
        let doc = doc! {
            "ts": Timestamp{time:1479561033,increment:0},
            "op": "u",
            "ns": "foo.bar",
            "o2": {
                "_id": 1
            },
            "o": {
                "data": {
                    "foo": "baz"
                }
            }
        };
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::Update {
                timestamp: DateTime::from_millis(1479561033),
                namespace: "foo.bar".into(),
                document: doc! { "data": { "foo": "baz" } },
            }
        );
    }

    #[test]
    fn operation_converts_deletes() {
        let doc = doc! {
            "ts": Timestamp{time: 1661330782,increment:0},
            "op": "d",
            "ns": "foo.bar",
            "o": {
                "_id": 1
            }
        };
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::Delete {
                timestamp: DateTime::from_millis(1661330782),
                namespace: "foo.bar".into(),
                document: doc! { "_id": 1 },
            }
        );
    }

    #[test]
    fn operation_returns_unknown_operations() {
        let doc = doc! { "op": "x" };
        let operation = Operation::new(&doc);

        match operation {
            Err(Error::UnknownOperation(op)) => assert_eq!(op, "x"),
            _ => panic!("Expected unknown operation."),
        }
    }

    #[test]
    fn operation_returns_missing_fields() {
        let doc = doc! { "foo": "bar" };
        let operation = Operation::new(&doc);

        match operation {
            Err(Error::MissingField(err)) => assert_eq!(err, ValueAccessError::NotPresent),
            _ => panic!("Expected missing field."),
        }
    }
}
