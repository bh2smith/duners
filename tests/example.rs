use chrono::{DateTime, Utc};
use dotenv::dotenv;
use duners::{client::DuneClient, dateutil::datetime_from_str};
use serde::Deserialize;
use std::env;

// User must declare the expected query return fields and types!
#[derive(Deserialize, Debug, PartialEq)]
struct ResultStruct {
    text_field: String,
    number_field: f64,
    #[serde(deserialize_with = "datetime_from_str")]
    date_field: DateTime<Utc>,
    list_field: String,
}

#[tokio::test]
async fn test_external_use() {
    dotenv().ok();
    let dune = DuneClient::new(env::var("DUNE_API_KEY").unwrap());
    let results = dune
        .refresh::<ResultStruct>(1215383, None, None)
        .await
        .unwrap();
    println!("{:?}", results.get_rows());
}
