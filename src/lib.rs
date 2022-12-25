#![allow(dead_code)]

extern crate core;

pub mod parameters;
mod response;
mod util;

use crate::response::{
    CancellationResponse, DuneError, ExecutionResponse, GetResultResponse, GetStatusResponse,
};
use reqwest::{Error, Response};
use serde::de::DeserializeOwned;

const BASE_URL: &str = "https://api.dune.com/api/v1";

pub struct DuneClient {
    api_key: String,
}

#[derive(Debug, PartialEq)]
enum DuneRequestError {
    // Includes known errors:
    // "invalid API Key"
    // "Query not found"
    // "The requested execution ID (ID: wonky job ID) is invalid."
    Dune(String),
    // Stuff coming from reqwest::Error
    Request(String),
    Server(String),
}

impl From<DuneError> for DuneRequestError {
    fn from(value: DuneError) -> Self {
        DuneRequestError::Dune(value.error)
    }
}

impl From<Error> for DuneRequestError {
    fn from(value: Error) -> Self {
        DuneRequestError::Request(value.to_string())
    }
}

impl DuneClient {
    async fn _post(&self, route: &str) -> Result<Response, Error> {
        let request_url = format!("{BASE_URL}/{route}");
        let client = reqwest::Client::new();
        client
            .post(&request_url)
            .header("x-dune-api-key", &self.api_key)
            .send()
            .await
    }

    async fn _get(&self, job_id: &str, command: &str) -> Result<Response, Error> {
        let request_url = format!("{BASE_URL}/execution/{job_id}/{command}");
        println!("{}", request_url);
        let client = reqwest::Client::new();
        client
            .get(&request_url)
            .header("x-dune-api-key", &self.api_key)
            .send()
            .await
    }

    async fn _parse_response<T: DeserializeOwned>(resp: Response) -> Result<T, DuneRequestError> {
        if resp.status().is_success() {
            resp.json::<T>().await.map_err(DuneRequestError::from)
        } else if resp.status().is_server_error() {
            Err(DuneRequestError::Server(resp.text().await?))
        } else {
            let err = resp
                .json::<DuneError>()
                .await
                .map_err(DuneRequestError::from)?;
            Err(DuneRequestError::from(err))
        }
    }
    async fn execute_query(&self, query_id: u32) -> Result<ExecutionResponse, DuneRequestError> {
        let response = self
            ._post(&format!("query/{query_id}/execute"))
            .await
            .map_err(DuneRequestError::from)?;
        DuneClient::_parse_response::<ExecutionResponse>(response).await
    }

    async fn cancel_execution(
        &self,
        job_id: &str,
    ) -> Result<CancellationResponse, DuneRequestError> {
        let response = self
            ._post(&format!("execution/{job_id}/cancel"))
            .await
            .map_err(DuneRequestError::from)?;
        DuneClient::_parse_response::<CancellationResponse>(response).await
    }

    async fn get_status(&self, job_id: &str) -> Result<GetStatusResponse, DuneRequestError> {
        let response = self
            ._get(job_id, "status")
            .await
            .map_err(DuneRequestError::from)?;
        DuneClient::_parse_response::<GetStatusResponse>(response).await
    }

    async fn get_results<T: DeserializeOwned>(
        &self,
        job_id: &str,
    ) -> Result<GetResultResponse<T>, DuneRequestError> {
        let response = self
            ._get(job_id, "results")
            .await
            .map_err(DuneRequestError::from)?;
        DuneClient::_parse_response::<GetResultResponse<T>>(response).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::response::ExecutionStatus;
    use dotenv::dotenv;
    use serde::Deserialize;
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
    async fn invalid_api_key() {
        let dune = DuneClient {
            api_key: "Baloney".parse().unwrap(),
        };
        let error = dune.execute_query(QUERY_ID).await.unwrap_err();
        assert_eq!(
            error,
            DuneRequestError::Dune(String::from("invalid API Key"))
        )
    }

    #[tokio::test]
    async fn invalid_query_id() {
        let dune = get_dune();
        let error = dune.execute_query(u32::MAX).await.unwrap_err();
        assert_eq!(
            error,
            DuneRequestError::Dune(String::from("Query not found"))
        )
    }

    #[tokio::test]
    async fn invalid_job_id() {
        let dune = get_dune();
        let error = dune
            .get_results::<DuneError>("wonky job ID")
            .await
            .unwrap_err();
        assert_eq!(
            error,
            DuneRequestError::Dune(String::from(
                "The requested execution ID (ID: wonky job ID) is invalid."
            ))
        )
    }

    #[tokio::test]
    async fn execute_query() {
        let dune = get_dune();
        let exec = dune.execute_query(QUERY_ID).await.unwrap();
        // Also testing cancellation!
        let cancellation = dune.cancel_execution(&exec.execution_id).await.unwrap();
        assert!(cancellation.success);
    }

    #[tokio::test]
    async fn get_status() {
        let dune = get_dune();
        let status = dune.get_status(JOB_ID).await.unwrap();
        assert_eq!(status.state, ExecutionStatus::Complete)
    }

    #[tokio::test]
    async fn get_results() {
        let dune = get_dune();

        #[derive(Deserialize, Debug)]
        struct ExpectedResults {
            token: String,
            symbol: String,
            max_price: f64,
        }

        let results = dune.get_results::<ExpectedResults>(JOB_ID).await.unwrap();
        // Query is for the max ETH price (should only have 1 result)
        let rows = results.result.rows;
        assert_eq!(1, rows.len());
        assert_eq!(rows[0].symbol, "WETH");
        assert_eq!(rows[0].token, "\\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2");
        assert!(rows[0].max_price > 4148.0)
    }
}
