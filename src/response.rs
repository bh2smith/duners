use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{de, Deserialize, Deserializer};

#[derive(Deserialize, Debug)]
pub struct ExecutionResponse {
    pub(crate) execution_id: String,
    // TODO use ExecutionState Enum
    state: String,
}

#[derive(Deserialize, Debug)]
pub struct CancellationResponse {
    pub(crate) success: bool,
}

#[derive(Deserialize, Debug)]
pub struct ResultMetaData {
    column_names: Vec<String>,
    result_set_bytes: u16,
    total_row_count: u32,
    datapoint_count: u32,
    pending_time_millis: Option<u32>,
    execution_time_millis: u32,
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
    submitted_at: DateTime<Utc>,
    #[serde(deserialize_with = "datetime_from_str")]
    expires_at: DateTime<Utc>,
    #[serde(deserialize_with = "datetime_from_str")]
    execution_started_at: DateTime<Utc>,
    #[serde(deserialize_with = "datetime_from_str")]
    execution_ended_at: DateTime<Utc>,
}

#[derive(Deserialize, Debug)]
pub struct GetStatusResponse {
    execution_id: String,
    query_id: u32,
    state: String,
    #[serde(flatten)]
    times: ExecutionTimes,
    queue_position: Option<u32>,
    result_metadata: Option<ResultMetaData>,
}

#[derive(Deserialize, Debug)]
pub struct ExecutionResult<T> {
    rows: Vec<T>,
    metadata: ResultMetaData,
}

#[derive(Deserialize, Debug)]
pub struct GetResultResponse<T> {
    execution_id: String,
    query_id: u32,
    state: String,
    // TODO - this `flatten` isn't what I had hoped for.
    //  I want the `times` field to disappear
    //  and all sub-fields to be brought up to this layer.
    #[serde(flatten)]
    times: ExecutionTimes,
    result: ExecutionResult<T>,
}
