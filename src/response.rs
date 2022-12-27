use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{de, Deserialize, Deserializer};
use serde_with::DeserializeFromStr;
use std::str::FromStr;

/// Encapsulates any "unexpected" data
/// returned from Dune upon bad request.
#[derive(Deserialize, Debug)]
pub struct DuneError {
    pub error: String,
}

/// Returned from successful call to `DuneClient::execute_query`
#[derive(Deserialize, Debug)]
pub struct ExecutionResponse {
    pub execution_id: String,
    pub state: ExecutionStatus,
}

/// Represents all possible states of query execution.
/// Most states are self explanatory.
/// Failure can occur if query takes too long (30 minutes) to execute.
/// Pending state also comes along with a "queue position"
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
    /// utility method for terminal query execution status.
    /// The three terminal states are complete, cancelled and failed.
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

/// Returned from call to `DuneClient::cancel_execution`
#[derive(Deserialize, Debug)]
pub struct CancellationResponse {
    /// true when cancellation was successful, otherwise false.
    pub success: bool,
}

/// Meta content returned optionally
/// with [GetStatusResponse](GetStatusResponse)
/// and always contained in [ExecutionResult](ExecutionResult).
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

/// Nested inside [GetStatusResponse](GetStatusResponse)
/// and [GetResultResponse](GetResultResponse).
/// Contains several UTC timestamps related to the query execution.
#[derive(Deserialize, Debug)]
pub struct ExecutionTimes {
    /// Time when query execution was submitted.
    #[serde(deserialize_with = "datetime_from_str")]
    pub submitted_at: DateTime<Utc>,
    /// Time when execution results will no longer be stored on Dune servers.
    #[serde(deserialize_with = "datetime_from_str")]
    pub expires_at: DateTime<Utc>,
    /// Time when query execution began.
    /// Differs from `submitted_at` if execution was pending in the queue.
    #[serde(deserialize_with = "datetime_from_str")]
    pub execution_started_at: DateTime<Utc>,
    /// Time that query execution completed.
    #[serde(deserialize_with = "datetime_from_str")]
    pub execution_ended_at: DateTime<Utc>,
}

/// Returned by successful call to `DuneClient::get_status`.
/// Indicates the current state of execution along with some metadata.
#[derive(Deserialize, Debug)]
pub struct GetStatusResponse {
    pub execution_id: String,
    pub query_id: u32,
    pub state: ExecutionStatus,
    #[serde(flatten)]
    pub times: ExecutionTimes,
    /// If the query state is Pending,
    /// then there will be an associated integer indicating queue position.
    pub queue_position: Option<u32>,
    /// This field will be non-empty once query execution has completed.
    pub result_metadata: Option<ResultMetaData>,
}

/// Contains the query results along with some additional metadata.
/// This struct is nested inside [GetResultResponse](GetResultResponse)
/// as the `result` field.
#[derive(Deserialize, Debug)]
pub struct ExecutionResult<T> {
    pub rows: Vec<T>,
    pub metadata: ResultMetaData,
}

/// Returned by a successful call to `DuneClient::get_results`.
/// Contains similar information to [GetStatusResponse](GetStatusResponse)
/// except that [ResultMetaData](ResultMetaData) is contained within the `result` field.
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
    /// Convenience method for fetching the "deeply" nested `rows` of the result response.
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
        assert!(ExecutionStatus::Complete.is_terminal());
        assert!(ExecutionStatus::Cancelled.is_terminal());
        assert!(ExecutionStatus::Failed.is_terminal());

        assert!(!ExecutionStatus::Pending.is_terminal());
        assert!(!ExecutionStatus::Executing.is_terminal());
    }
}
