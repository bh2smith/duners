# duners

A convenience library for executing queries and recovering results from Dune Analytics API.

## Installation and Usage

```shell
cargo add duners
```

```rust
use chrono::{DateTime, Utc};
use duners::{client::DuneClient, dateutil::datetime_from_str};
use serde::Deserialize;

// User must declare the expected query return fields and types!
#[derive(Deserialize, Debug, PartialEq)]
struct ResultStruct {
    text_field: String,
    number_field: f64,
    #[serde(deserialize_with = "datetime_from_str")]
    date_field: DateTime<Utc>,
    list_field: String,
}

#[tokio::main]
async fn main() -> Result<(), DuneRequestError> {
    let dune = DuneClient::from_env();
    let results = dune.refresh::<ResultStruct>(1215383, None, None).await?;
    println!("{:?}", results.get_rows());
    Ok(())
}
```
