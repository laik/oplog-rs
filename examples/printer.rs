use futures::{pin_mut, StreamExt};
use mongodb::Client;
use oplog::{Operation, Oplog};

#[tokio::main]
async fn main() {
    env_logger::init();
    let client = Client::with_uri_str("mongodb://127.0.0.1:27017")
        .await
        .unwrap();

    let mut oplog = match Oplog::new(&client).await {
        Ok(it) => it,
        _ => return,
    };

    let stream = oplog.stream();

    pin_mut!(stream);

    while let Some(op) = stream.next().await {
        match op {
            Operation::Insert {
                timestamp,
                document,
                ..
            } => println!("Insert at {} {:?}", timestamp, document),
            Operation::Update {
                timestamp, document, ..
            } => println!("Update at {} {:?}", timestamp, document),
            Operation::Delete {
                timestamp,
                document,
                ..
            } => println!("Delete at {} {:?}", timestamp, document),
        }
    }
}
