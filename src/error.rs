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
    /// Errors bubbled up from Serde (de)serialization
    Serde(String),
    /// Errors bubbled up from PolarsError
    Polars(String),
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

impl From<polars::error::PolarsError> for DuneRequestError {
    fn from(value: polars::error::PolarsError) -> Self {
        DuneRequestError::Polars(value.to_string())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use maplit::hashmap;
    use serde::Serialize;

    #[tokio::test]
    async fn async_error_parsing() {
        let request_error = reqwest::get("invalid-url").await.unwrap_err();
        assert_eq!(
            DuneRequestError::from(request_error),
            DuneRequestError::Request("builder error: relative URL without a base".to_string())
        );
    }
    #[test]
    fn standard_error_parsing() {
        assert_eq!(
            DuneRequestError::from(DuneError {
                error: "broken".to_string()
            }),
            DuneRequestError::Dune("broken".to_string())
        );

        // An example where serde_json::to_string fails.
        // https://www.greyblake.com/blog/when-serde-json-to-string-fails/
        #[derive(Serialize, Eq, PartialEq, Hash)]
        #[serde(tag = "t")]
        enum TestEnum {
            Item,
        }
        let serde_error = serde_json::to_string(&hashmap! {
            TestEnum::Item => 2,
        })
        .unwrap_err();
        assert_eq!(
            DuneRequestError::from(serde_error),
            DuneRequestError::Serde("key must be a string".to_string())
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
