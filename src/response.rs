use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{de, Deserialize, Deserializer};

#[derive(Deserialize, Debug)]
pub struct DuneError {
    pub(crate) error: String,
}

#[derive(Deserialize, Debug)]
pub struct ExecutionResponse {
    pub(crate) execution_id: String,
    // TODO use ExecutionState Enum
    pub state: String,
}

#[derive(Deserialize, Debug)]
pub struct CancellationResponse {
    pub(crate) success: bool,
}

#[derive(Deserialize, Debug)]
pub struct ResultMetaData {
    pub column_names: Vec<String>,
    pub result_set_bytes: u16,
    pub total_row_count: u32,
    pub datapoint_count: u32,
    pub pending_time_millis: Option<u32>,
    pub execution_time_millis: u32,
}

fn datetime_from_str<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    // Example: 2022-12-23T10:34:06.129331594Z
    let native =
        NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S.%fZ").map_err(de::Error::custom);
    Ok(DateTime::<Utc>::from_utc(native?, Utc))
}

#[derive(Deserialize, Debug)]
pub struct ExecutionTimes {
    #[serde(deserialize_with = "datetime_from_str")]
    pub submitted_at: DateTime<Utc>,
    #[serde(deserialize_with = "datetime_from_str")]
    pub expires_at: DateTime<Utc>,
    #[serde(deserialize_with = "datetime_from_str")]
    pub execution_started_at: DateTime<Utc>,
    #[serde(deserialize_with = "datetime_from_str")]
    pub execution_ended_at: DateTime<Utc>,
}

#[derive(Deserialize, Debug)]
pub struct GetStatusResponse {
    pub execution_id: String,
    pub query_id: u32,
    pub state: String,
    #[serde(flatten)]
    pub times: ExecutionTimes,
    pub queue_position: Option<u32>,
    pub result_metadata: Option<ResultMetaData>,
}

#[derive(Deserialize, Debug)]
pub struct ExecutionResult<T> {
    pub rows: Vec<T>,
    pub metadata: ResultMetaData,
}

#[derive(Deserialize, Debug)]
pub struct GetResultResponse<T> {
    pub execution_id: String,
    pub query_id: u32,
    pub state: String,
    // TODO - this `flatten` isn't what I had hoped for.
    //  I want the `times` field to disappear
    //  and all sub-fields to be brought up to this layer.
    #[serde(flatten)]
    pub times: ExecutionTimes,
    pub result: ExecutionResult<T>,
}

impl<T> GetResultResponse<T> {
    pub fn get_rows(self) -> Vec<T> {
        self.result.rows
    }
}
