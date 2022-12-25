#![allow(dead_code)]

extern crate core;

pub mod parameters;
mod response;
mod util;

use crate::parameters::Parameter;
use crate::response::{
    CancellationResponse, DuneError, ExecutionResponse, ExecutionStatus, GetResultResponse,
    GetStatusResponse,
};
use reqwest::{Error, Response};
use serde::de::DeserializeOwned;
use serde_json::json;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};

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
    async fn _post(&self, route: &str, params: Option<Vec<Parameter>>) -> Result<Response, Error> {
        let params = params
            .unwrap_or_default()
            .into_iter()
            .map(|p| (p.key, p.value))
            .collect::<HashMap<_, _>>();
        let request_url = format!("{BASE_URL}/{route}");
        let client = reqwest::Client::new();
        client
            .post(&request_url)
            .header("x-dune-api-key", &self.api_key)
            .json(&json!({ "query_parameters": params }))
            .send()
            .await
    }

    async fn _get(&self, job_id: &str, command: &str) -> Result<Response, Error> {
        let request_url = format!("{BASE_URL}/execution/{job_id}/{command}");
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
    async fn execute_query(
        &self,
        query_id: u32,
        params: Option<Vec<Parameter>>,
    ) -> Result<ExecutionResponse, DuneRequestError> {
        let response = self
            ._post(&format!("query/{query_id}/execute"), params)
            .await
            .map_err(DuneRequestError::from)?;
        DuneClient::_parse_response::<ExecutionResponse>(response).await
    }

    async fn cancel_execution(
        &self,
        job_id: &str,
    ) -> Result<CancellationResponse, DuneRequestError> {
        let response = self
            ._post(&format!("execution/{job_id}/cancel"), None)
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

    async fn refresh<T: DeserializeOwned>(
        &self,
        query_id: u32,
        parameters: Option<Vec<Parameter>>,
        ping_frequency: Option<u64>,
    ) -> Result<GetResultResponse<T>, DuneRequestError> {
        let job_id = self.execute_query(query_id, parameters).await?.execution_id;
        let mut status = self.get_status(&job_id).await?;
        while !status.state.is_terminal() {
            // TODO - use logger!
            println!(
                "waiting for query execution {job_id} to complete: {:?}",
                status.state
            );
            sleep(Duration::from_secs(ping_frequency.unwrap_or(5))).await;
            status = self.get_status(&job_id).await?
        }
        let full_response = self.get_results::<T>(&job_id).await;
        if status.state == ExecutionStatus::Failed {
            println!("ERROR - Execution Failed: Perhaps your query took too long to run!")
        }
        full_response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::response::ExecutionStatus;
    use crate::util::date_parse;
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
        let error = dune.execute_query(QUERY_ID, None).await.unwrap_err();
        assert_eq!(
            error,
            DuneRequestError::Dune(String::from("invalid API Key"))
        )
    }

    #[tokio::test]
    async fn invalid_query_id() {
        let dune = get_dune();
        let error = dune.execute_query(u32::MAX, None).await.unwrap_err();
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
        let exec = dune.execute_query(QUERY_ID, None).await.unwrap();
        // Also testing cancellation!
        let cancellation = dune.cancel_execution(&exec.execution_id).await.unwrap();
        assert!(cancellation.success);
    }

    #[tokio::test]
    async fn execute_query_with_params() {
        let dune = get_dune();
        let all_parameter_types = vec![
            Parameter::date("DateField", date_parse("2022-05-04T00:00:00.0Z").unwrap()),
            Parameter::number("NumberField", "3.1415926535"),
            Parameter::text("TextField", "Plain Text"),
            Parameter::list("ListField", "Option 1"),
        ];
        let exec_result = dune.execute_query(1215383, Some(all_parameter_types)).await;
        assert!(exec_result.is_ok())
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

    #[tokio::test]
    async fn refresh() {
        let dune = get_dune();

        #[derive(Deserialize, Debug, PartialEq)]
        struct ResultStruct {
            text_field: String,
            number_field: String,
            date_field: String,
            list_field: String,
        }
        let results = dune
            .refresh::<ResultStruct>(1215383, None, None)
            .await
            .unwrap();
        assert_eq!(
            ResultStruct {
                text_field: "Plain Text".to_string(),
                number_field: "3.1415926535".to_string(),
                date_field: "2022-05-04 00:00:00".to_string(),
                list_field: "Option 1".to_string(),
            },
            results.get_rows()[0]
        )
    }
}
