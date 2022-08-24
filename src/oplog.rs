//! The oplog module is responsible for building an iterator over a MongoDB replica set oplog with
//! any optional filtering criteria applied.

use async_stream::stream;
use futures::{pin_mut, Stream, StreamExt, TryStreamExt};
use mongodb::bson::{doc, Document};
use mongodb::options::{CursorType, FindOptions};
use mongodb::{Client, Cursor};

pub struct Oplog {
    cursor: Cursor<Document>,
}

impl Oplog {
    pub async fn new(client: &Client) -> crate::Result<Oplog> {
        let oplog = OplogBuilder::new(client)
            .filter(Some(doc! {"op":{"$in":["d","u","i"]}}))
            .build()
            .await;
        oplog
    }

    pub async fn print(&mut self) {
        loop {
            match self.cursor.try_next().await {
                Ok(o) => {
                    if let Some(o) = o {
                        println!("{:?}", crate::Operation::new(&o).unwrap())
                    }
                }
                Err(e) => println!("{:?}", e),
            }
        }
    }

    /// iter Example:
    pub fn stream<'a>(&'a mut self) -> impl Stream<Item = crate::Operation> + 'a {
        let block = stream! {
             loop{
                match self.cursor.try_next().await{
                    Ok(o) => {
                        if let Some(o) = o{
                            yield crate::Operation::new(&o).unwrap()
                        }
                    },
                    Err(e) => println!("{:?}",e),
                }
            }
        };
        block
    }
}

#[derive(Clone)]
pub struct OplogBuilder<'a> {
    client: &'a Client,
    filter: Option<Document>,
}

impl<'a> OplogBuilder<'a> {
    pub fn new(client: &'a Client) -> OplogBuilder<'a> {
        OplogBuilder {
            client: client,
            filter: None,
        }
    }

    pub async fn build(&self) -> crate::Result<Oplog> {
        let coll = self.client.database("local").collection("oplog.rs");

        let opts = FindOptions::builder()
            .cursor_type(CursorType::TailableAwait)
            .no_cursor_timeout(true)
            .build();

        let cursor = coll
            .find(self.filter.clone(), opts)
            .await
            .map_err(|e| crate::Error::Database(e))?;

        Ok(Oplog { cursor })
    }

    #[allow(dead_code)]
    pub fn filter(&mut self, filter: Option<Document>) -> &mut OplogBuilder<'a> {
        self.filter = filter;
        self
    }
}
