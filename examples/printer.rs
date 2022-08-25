use mongodb::Client;
use oplog::subscribe;
use tokio_context::context::Context;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Gps {
    #[serde(alias = "_id")]
    uid: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    gps: Vec<Vec<f64>>,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let client = Client::with_uri_str("mongodb://10.200.100.200:27017")
        .await
        .unwrap();

    let (ctx, handle) = Context::new();
    let mut oplog = subscribe::<Gps>(ctx, client, "base", "gps_latest", None).unwrap();

    while let Some(op) = oplog.recv().await {
        println!("{}", op);
    }

    handle.cancel();
}
