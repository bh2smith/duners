#![allow(dead_code)]

mod response;

use crate::response::{
    CancellationResponse, ExecutionResponse, GetResultResponse, GetStatusResponse,
};
use reqwest::{Error as RequestError, Response};
use serde::de::DeserializeOwned;

const BASE_URL: &str = "https://api.dune.com/api/v1";

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

    async fn get_results<T: DeserializeOwned>(
        &self,
        job_id: &str,
    ) -> Result<GetResultResponse<T>, RequestError> {
        let response = self._get(job_id, "results").await?;
        response.json::<GetResultResponse<T>>().await
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
