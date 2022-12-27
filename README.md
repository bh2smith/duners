# duners

A convenience library for executing queries and recovering results from Dune Analytics API.

## Installation and Usage

```shell
cargo add duners
```

```rust
use std::env;
use dotenv::dotenv;
use duners::client::{DuneClient, DuneRequestError};
use serde::Deserialize;

// User must declare the expected query return types and fields.
#[derive(Deserialize, Debug, PartialEq)]
struct ResultStruct {
    text_field: String,
    number_field: String,
    date_field: String,
    list_field: String,
}

#[tokio::main]
async fn main() -> Result<(), DuneRequestError> {
    dotenv().ok();
    let dune = DuneClient::new(env::var("DUNE_API_KEY").unwrap());
    let results = dune.refresh::<ResultStruct>(1215383, None, None).await?;
    println!("{:?}", results.get_rows());
    Ok(())
}
```
