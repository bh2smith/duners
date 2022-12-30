use serde::Deserialize;

/// Encapsulates any "unexpected" data
/// returned from Dune upon bad request.
#[derive(Deserialize, Debug)]
pub struct DuneError {
    pub error: String,
}

#[derive(Debug, PartialEq)]
pub enum DuneRequestError {
    /// Includes known errors:
    /// "invalid API Key"
    /// "Query not found"
    /// "The requested execution ID (ID: wonky job ID) is invalid."
    Dune(String),
    /// Errors bubbled up from reqwest::Error
    Request(String),
    // /// Errors bubbled up from Serde (de)serialization
    // Serde(String),
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

// impl From<serde_json::Error> for DuneRequestError {
//     fn from(value: serde_json::Error) -> Self {
//         DuneRequestError::Serde(value.to_string())
//     }
// }

// impl From<polars::error::PolarsError> for DuneRequestError {
//     fn from(value: polars::error::PolarsError) -> Self {
//         DuneRequestError::Polars(value.to_string())
//     }
// }
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn error_parsing() {
        let err = reqwest::get("invalid-url").await.unwrap_err();
        assert_eq!(
            DuneRequestError::from(err),
            DuneRequestError::Request("builder error: relative URL without a base".to_string())
        );
        assert_eq!(
            DuneRequestError::from(DuneError {
                error: "broken".to_string()
            }),
            DuneRequestError::Dune("broken".to_string())
        )
    }

    #[test]
    fn derive_debug() {
        assert_eq!(
            format!(
                "{:?}",
                DuneError {
                    error: "broken".to_string()
                }
            ),
            "DuneError { error: \"broken\" }"
        );
    }
}
