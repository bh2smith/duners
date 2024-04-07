use crate::{client::DuneClient, error::DuneRequestError, parameters::Parameter};
use polars::{
    frame::DataFrame,
    prelude::{JsonReader, SerReader},
};
use serde::{de::DeserializeOwned, Serialize};
use std::io::Cursor;

impl DuneClient {
    pub async fn fetch_as_dataframe<T: DeserializeOwned + Serialize>(
        &self,
        query_id: u32,
        parameters: Option<Vec<Parameter>>,
        ping_frequency: Option<u64>,
    ) -> Result<DataFrame, DuneRequestError> {
        let results = self
            .refresh::<T>(query_id, parameters, ping_frequency)
            .await?
            .get_rows();
        let json = serde_json::to_string(&results).map_err(DuneRequestError::from)?;

        let cursor = Cursor::new(json);

        Ok(JsonReader::new(cursor).finish()?)
    }
}

#[cfg(test)]
mod tests {
    use crate::{client::DuneClient, parse_utils::datetime_from_str};
    use chrono::{DateTime, Utc};
    use polars::export::ahash::HashMap;
    use serde::{Deserialize, Serialize};

    #[tokio::test]
    async fn fetch_as_dataframe() {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct ResultStruct {
            text_field: String,
            number_field: f64,
            #[serde(deserialize_with = "datetime_from_str")]
            date_field: DateTime<Utc>,
            list_field: String,
        }

        let dune = DuneClient::from_env();

        let df = dune
            .fetch_as_dataframe::<ResultStruct>(1215383, None, None)
            .await
            .unwrap();
        println!("{:?}", df);
    }

    #[tokio::test]
    async fn lazy_fetch_as_dataframe() {
        let dune = DuneClient::from_env();
        // This query is a fork of the above, with all string fields.
        let df = dune
            .fetch_as_dataframe::<HashMap<String, String>>(1832271, None, None)
            .await
            .unwrap();
        println!("{:?}", df);
    }
}
