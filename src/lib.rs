// #![warn(missing_docs)]
#![feature(async_iterator)]

use std::fmt;
use std::result;

use futures::pin_mut;
use futures::StreamExt;
use mongodb::bson::doc;
use mongodb::bson::document::ValueAccessError;
use mongodb::bson::Document;
use mongodb::Client;
pub(crate) use operation::Operation;
pub(crate) use oplog::Oplog;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc::Receiver;
use tokio_context::context::Context;

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

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub enum Event<T> {
    Added(T),
    Updated(T),
    Deleted(T),
    Error(String),
}

impl<T> std::fmt::Display for Event<T>
where
    T: std::fmt::Debug + serde::Serialize,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::Added(ref t) => write!(
                f,
                r#"{{ \"type\": \"ADDED\", \"object\": {:?} }}"#,
                serde_json::to_string(&t).unwrap()
            ),
            Event::Updated(t) => write!(
                f,
                r#"{{ \"type\": \"MODIFIED\", \"object\": {:?} }}"#,
                serde_json::to_string(&t).unwrap()
            ),
            Event::Deleted(t) => write!(
                f,
                r#"{{ \"type\": \"DELETED\", \"object\": {:?} }}"#,
                serde_json::to_string(&t).unwrap()
            ),
            Event::Error(ref s) => write!(f, r#"{{ \"type\": \"ERROR\", \"msg\": {:?} }}"#, s),
        }
    }
}

pub fn subscribe<'a, T>(
    ctx: Context,
    client: Client,
    ns: &str,
    coll: &str,
    filter: Option<Document>,
) -> Result<Receiver<Event<T>>>
where
    T: core::fmt::Debug + DeserializeOwned + Send + Sync + 'static,
{
    let ns = format!("{}.{}", ns, coll);
    let filter = if let Some(mut filter) = filter {
        filter.insert("ns", ns);
        filter
    } else {
        doc! {"ns":ns}
    };
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tokio::spawn(async move {
        let block = async move {
            let mut oplog = Oplog::new(&client, filter).await.unwrap();

            let stream = oplog.stream();
            pin_mut!(stream);

            while let Some(op) = stream.next().await {
                let evt = match op {
                    Operation::Insert { document, .. } => {
                        match mongodb::bson::from_document::<T>(document) {
                            Ok(t) => Event::Added(t),
                            Err(e) => Event::Error(e.to_string()),
                        }
                    }
                    Operation::Update { document, .. } => {
                        match mongodb::bson::from_document::<T>(document) {
                            Ok(t) => Event::Updated(t),
                            Err(e) => Event::Error(e.to_string()),
                        }
                    }
                    Operation::Delete { document, .. } => {
                        match mongodb::bson::from_document::<T>(document) {
                            Ok(t) => Event::Deleted(t),
                            Err(e) => Event::Error(e.to_string()),
                        }
                    }
                };

                let _ = tx
                    .send(evt)
                    .await
                    .map_err(|e| log::error!("Error sending event: {}", e));
            }
        };

        let mut ctx = ctx;
        tokio::select! {
            _= block =>{},
            _= ctx.done() => {
                return;
            },
        }
    });

    Ok(rx)
}
