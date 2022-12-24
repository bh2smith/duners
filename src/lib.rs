#![allow(dead_code)]

extern crate core;

mod response;

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
            resp.json::<T>()
                .await
                .map_err(|e| DuneRequestError::from(e))
        } else if resp.status().is_server_error() {
            Err(DuneRequestError::Server(resp.text().await?))
        } else {
            let err = resp
                .json::<DuneError>()
                .await
                .map_err(|e| DuneRequestError::from(e))?;
            Err(DuneRequestError::from(err))
        }
    }
    async fn execute_query(&self, query_id: u32) -> Result<ExecutionResponse, DuneRequestError> {
        let response = self
            ._post(&format!("query/{query_id}/execute"))
            .await
            .map_err(|e| DuneRequestError::from(e))?;
        DuneClient::_parse_response::<ExecutionResponse>(response).await
    }

    async fn cancel_execution(
        &self,
        job_id: &str,
    ) -> Result<CancellationResponse, DuneRequestError> {
        let response = self
            ._post(&format!("execution/{job_id}/cancel"))
            .await
            .map_err(|e| DuneRequestError::from(e))?;
        DuneClient::_parse_response::<CancellationResponse>(response).await
    }

    async fn get_status(&self, job_id: &str) -> Result<GetStatusResponse, DuneRequestError> {
        let response = self
            ._get(job_id, "status")
            .await
            .map_err(|e| DuneRequestError::from(e))?;
        DuneClient::_parse_response::<GetStatusResponse>(response).await
    }

    async fn get_results<T: DeserializeOwned>(
        &self,
        job_id: &str,
    ) -> Result<GetResultResponse<T>, DuneRequestError> {
        let response = self
            ._get(job_id, "results")
            .await
            .map_err(|e| DuneRequestError::from(e))?;
        DuneClient::_parse_response::<GetResultResponse<T>>(response).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenv::dotenv;
    use serde::de::Unexpected::Str;
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
        // unwrap implies function success.
        let error = dune.execute_query(QUERY_ID).await.unwrap_err();
        assert_eq!(
            error,
            DuneRequestError::Dune(String::from("invalid API Key"))
        )
        // assert_eq!(error, Error { inner: "invalid API key" });
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
        // unwrap implies function success.
        let exec = dune.execute_query(QUERY_ID).await.unwrap();
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

        #[derive(Deserialize, Debug)]
        struct ExpectedResults {
            token: String,
            symbol: String,
            max_price: f64,
        }

        let results = dune.get_results::<ExpectedResults>(JOB_ID).await.unwrap();
        println!("{:?}", results);
    }
}
