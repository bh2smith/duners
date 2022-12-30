use crate::response::DuneError;

#[derive(Debug, PartialEq)]
pub enum DuneRequestError {
    /// Includes known errors:
    /// "invalid API Key"
    /// "Query not found"
    /// "The requested execution ID (ID: wonky job ID) is invalid."
    Dune(String),
    /// Errors bubbled up from reqwest::Error
    Request(String),
    /// Errors bubbled up from Serde (de)serialization
    Serde(String),
    // /// Errors bubbled up from PolarsError
    // Polars(String),
}

impl From<DuneError> for DuneRequestError {
    fn from(value: DuneError) -> Self {
        DuneRequestError::Dune(value.error)
    }
}

impl From<reqwest::Error> for DuneRequestError {
    fn from(value: reqwest::Error) -> Self {
        DuneRequestError::Request(value.to_string())
    }
}

impl From<serde_json::Error> for DuneRequestError {
    fn from(value: serde_json::Error) -> Self {
        DuneRequestError::Serde(value.to_string())
    }
}

// impl From<polars::error::PolarsError> for DuneRequestError {
//     fn from(value: polars::error::PolarsError) -> Self {
//         DuneRequestError::Polars(value.to_string())
//     }
// }
