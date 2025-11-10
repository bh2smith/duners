use std::fmt::Debug;
use crate::{client::DuneClient, error::DuneRequestError, parameters::Parameter};
use polars::{
    frame::DataFrame,
    prelude::{JsonReader, SerReader},
};
use serde::{de::DeserializeOwned, Serialize};
use std::io::Cursor;

impl DuneClient {
    pub async fn fetch_as_dataframe<T: DeserializeOwned + Serialize + Debug>(
        &self,
        query_id: u32,
        parameters: Option<Vec<Parameter>>,
        ping_frequency: Option<u64>,
    ) -> Result<DataFrame, DuneRequestError> {
        let results = self
            .refresh::<T>(query_id, parameters, ping_frequency)
            .await?;
        let json = serde_json::to_string(&results.get_rows()).map_err(DuneRequestError::from)?;

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
        // Response here should be: http://jsonblob.com/1226634378817167360
        // {"execution_id":"01HTX4625Y4PA8CHCZWSB5NA0F","query_id":1215383,"is_execution_finished":true,"state":"QUERY_STATE_COMPLETED","submitted_at":"2024-04-07T20:32:19.134841Z","expires_at":"2024-07-06T20:32:19.553234Z","execution_started_at":"2024-04-07T20:32:19.406404Z","execution_ended_at":"2024-04-07T20:32:19.553232Z","result":{"rows":[{"date_field":"2022-05-04 00:00:00.000","list_field":"Option 1","number_field":"3.1415926535","text_field":"Plain Text"}],"metadata":{"column_names":["text_field","number_field","date_field","list_field"],"row_count":1,"result_set_bytes":103,"total_row_count":1,"total_result_set_bytes":103,"datapoint_count":4,"pending_time_millis":271,"execution_time_millis":146}}}
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
