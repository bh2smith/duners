use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{de, Deserialize, Deserializer};
use serde_with::DeserializeFromStr;
use std::str::FromStr;

#[derive(Deserialize, Debug)]
pub struct DuneError {
    pub error: String,
}

#[derive(Deserialize, Debug)]
pub struct ExecutionResponse {
    pub execution_id: String,
    // TODO use ExecutionState Enum
    pub state: String,
}

#[derive(DeserializeFromStr, Debug, PartialEq)]
pub enum ExecutionStatus {
    Complete,
    Executing,
    Pending,
    Cancelled,
    Failed,
}

impl FromStr for ExecutionStatus {
    type Err = String;

    fn from_str(input: &str) -> Result<ExecutionStatus, Self::Err> {
        match input {
            "QUERY_STATE_COMPLETED" => Ok(ExecutionStatus::Complete),
            "QUERY_STATE_EXECUTING" => Ok(ExecutionStatus::Executing),
            "QUERY_STATE_PENDING" => Ok(ExecutionStatus::Pending),
            "QUERY_STATE_CANCELLED" => Ok(ExecutionStatus::Cancelled),
            "QUERY_STATE_FAILED" => Ok(ExecutionStatus::Failed),
            other => Err(format!("Parse Error {other}")),
        }
    }
}

impl ExecutionStatus {
    pub fn is_terminal(&self) -> bool {
        match self {
            ExecutionStatus::Complete => true,
            ExecutionStatus::Cancelled => true,
            ExecutionStatus::Failed => true,
            ExecutionStatus::Executing => false,
            ExecutionStatus::Pending => false,
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct CancellationResponse {
    pub success: bool,
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
    pub state: ExecutionStatus,
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
    pub state: ExecutionStatus,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_from_str() {
        assert_eq!(
            ExecutionStatus::from_str("invalid"),
            Err(String::from("Parse Error invalid"))
        );
        assert_eq!(
            ExecutionStatus::from_str("QUERY_STATE_COMPLETED"),
            Ok(ExecutionStatus::Complete)
        );
        assert_eq!(
            ExecutionStatus::from_str("QUERY_STATE_EXECUTING"),
            Ok(ExecutionStatus::Executing)
        );
        assert_eq!(
            ExecutionStatus::from_str("QUERY_STATE_PENDING"),
            Ok(ExecutionStatus::Pending)
        );
        assert_eq!(
            ExecutionStatus::from_str("QUERY_STATE_CANCELLED"),
            Ok(ExecutionStatus::Cancelled)
        );
        assert_eq!(
            ExecutionStatus::from_str("QUERY_STATE_FAILED"),
            Ok(ExecutionStatus::Failed)
        );
    }
    #[test]
    fn terminal_statuses() {
        assert_eq!(ExecutionStatus::Complete.is_terminal(), true);
        assert_eq!(ExecutionStatus::Cancelled.is_terminal(), true);
        assert_eq!(ExecutionStatus::Failed.is_terminal(), true);

        assert_eq!(ExecutionStatus::Pending.is_terminal(), false);
        assert_eq!(ExecutionStatus::Executing.is_terminal(), false);
    }
}
