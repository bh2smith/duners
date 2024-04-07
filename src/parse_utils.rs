use chrono::{DateTime, NaiveDateTime, ParseError, Utc};
use serde::{de, Deserialize, Deserializer};
use serde_json::Value;

fn date_string_parser(date_str: &str, format: &str) -> Result<DateTime<Utc>, ParseError> {
    let native = NaiveDateTime::parse_from_str(date_str, format);
    Ok(DateTime::from_naive_utc_and_offset(native?, Utc))
}

/// The date format returned by DuneAPI response Date fields (e.g. `submitted_at`)
pub fn date_parse(date_str: &str) -> Result<DateTime<Utc>, ParseError> {
    date_string_parser(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
}

/// The Date format returned from data fields of type timestamp.
pub fn dune_date(date_str: &str) -> Result<DateTime<Utc>, ParseError> {
    date_string_parser(date_str, "%Y-%m-%d %H:%M:%S.%f")
}

pub fn datetime_from_str<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    match date_parse(&s) {
        // First try to parse response type date strings
        Ok(parsed_date) => Ok(parsed_date),
        Err(_) => {
            // First attempt didn't work, try another format
            dune_date(&s).map_err(de::Error::custom)
        }
    }
}

pub fn optional_datetime_from_str<'de, D>(
    deserializer: D,
) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Deserialize::deserialize(deserializer)?;
    match s {
        None => Ok(None),
        Some(s) => {
            let date = date_parse(&s).map_err(de::Error::custom)?;
            Ok(Some(date))
        }
    }
}

pub fn f64_from_str<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Value = Deserialize::deserialize(deserializer)?;
    if let Value::String(s) = value {
        s.parse().map_err(de::Error::custom)
    } else {
        Err(de::Error::custom("Expected a string"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_parse_works() {
        let date_str = "2022-01-01T01:02:03.123Z";
        assert_eq!(
            date_parse(date_str).unwrap().to_string(),
            "2022-01-01 01:02:03.000000123 UTC"
        )
    }

    #[test]
    fn new_dune_date() {
        let date_str = "2022-05-04 00:00:00.000";
        assert_eq!(
            dune_date(date_str).unwrap().to_string(),
            "2022-05-04 00:00:00 UTC"
        )
    }
}
