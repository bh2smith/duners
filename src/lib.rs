#![allow(dead_code)]
use chrono::NaiveDateTime;
use reqwest::{Error as RequestError, Response};
use serde::{de, Deserialize, Deserializer};

const BASE_URL: &str = "https://api.dune.com/api/v1";

#[derive(Deserialize, Debug)]
struct ExecutionResponse {
    execution_id: String,
    // TODO use ExecutionState Enum
    state: String,
}

#[derive(Deserialize, Debug)]
struct CancellationResponse {
    success: bool,
}
#[derive(Deserialize, Debug)]
struct ResultMetaData {
    column_names: Vec<String>,
    result_set_bytes: u16,
    total_row_count: u32,
    datapoint_count: u32,
    pending_time_millis: Option<u32>,
    execution_time_millis: u32,
}

fn datetime_from_str<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    // 2022-12-23T10:34:06.129331594Z
    // TODO - use DateTime<Utc>
    NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S.%fZ").map_err(de::Error::custom)
}

#[derive(Deserialize, Debug)]
struct ExecutionTimes {
    // TODO - use DateTime<UTC>
    #[serde(deserialize_with = "datetime_from_str")]
    submitted_at: NaiveDateTime,
    #[serde(deserialize_with = "datetime_from_str")]
    expires_at: NaiveDateTime,
    #[serde(deserialize_with = "datetime_from_str")]
    execution_started_at: NaiveDateTime,
    #[serde(deserialize_with = "datetime_from_str")]
    execution_ended_at: NaiveDateTime,
}

#[derive(Deserialize, Debug)]
struct GetStatusResponse {
    execution_id: String,
    query_id: u32,
    state: String,
    #[serde(flatten)]
    times: ExecutionTimes,
    queue_position: Option<u32>,
    result_metadata: Option<ResultMetaData>,
}

#[derive(Deserialize, Debug)]
struct ExecutionResult {
    // TODO - how to parse Unknown Result Types?
    // rows: Vec<T>,
    metadata: ResultMetaData,
}

#[derive(Deserialize, Debug)]
struct GetResultResponse {
    execution_id: String,
    query_id: u32,
    state: String,
    // TODO - this `flatten` isn't what I had hoped for.
    //  I want the `times` field to disappear
    //  and all sub-fields to be brought up to this layer.
    #[serde(flatten)]
    times: ExecutionTimes,
    result: ExecutionResult,
}

struct DuneClient {
    api_key: String,
}

impl DuneClient {
    async fn _post(&self, route: &str) -> Result<Response, RequestError> {
        let request_url = format!("{BASE_URL}/{route}");
        let client = reqwest::Client::new();
        client
            .post(&request_url)
            .header("x-dune-api-key", &self.api_key)
            .send()
            .await
    }

    async fn _get(&self, job_id: &str, command: &str) -> Result<Response, RequestError> {
        let request_url = format!("{BASE_URL}/execution/{job_id}/{command}");
        println!("{}", request_url);
        let client = reqwest::Client::new();
        client
            .get(&request_url)
            .header("x-dune-api-key", &self.api_key)
            .send()
            .await
    }

    async fn execute_query(&self, query_id: u32) -> Result<ExecutionResponse, RequestError> {
        let response = self._post(&format!("query/{query_id}/execute")).await?;
        response.json::<ExecutionResponse>().await
    }

    async fn cancel_execution(&self, job_id: &str) -> Result<CancellationResponse, RequestError> {
        let response = self._post(&format!("execution/{job_id}/cancel")).await?;
        let json = response.json().await;
        println!("{:?}", json);
        json as Result<CancellationResponse, RequestError>
    }

    async fn get_status(&self, job_id: &str) -> Result<GetStatusResponse, RequestError> {
        let response = self._get(job_id, "status").await?;
        response.json::<GetStatusResponse>().await
    }

    async fn get_results(&self, job_id: &str) -> Result<GetResultResponse, RequestError> {
        let response = self._get(job_id, "results").await?;
        response.json::<GetResultResponse>().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenv::dotenv;
    use std::env;

    const QUERY_ID: u32 = 971694;
    const JOB_ID: &str = "01GMZ8R4NPPQZCWYJRY2K03MH0";

    fn get_dune() -> DuneClient {
        dotenv().ok();
        DuneClient {
            api_key: env::var("DUNE_API_KEY").unwrap(),
        }
    }

    #[tokio::test]
    async fn execute_query() {
        let dune = get_dune();
        let exec = dune.execute_query(QUERY_ID).await.unwrap();
        println!("{:?}", exec);
        let cancellation = dune.cancel_execution(&exec.execution_id).await.unwrap();
        assert_eq!(cancellation.success, true);
    }

    #[tokio::test]
    async fn get_status() {
        let dune = get_dune();
        let status = dune.get_status(JOB_ID).await.unwrap();
        println!("{:?}", status);
    }

    #[tokio::test]
    async fn get_results() {
        let dune = get_dune();
        let results = dune.get_results(JOB_ID).await.unwrap();
        println!("{:?}", results);
    }
}
