use chrono::{NaiveDateTime, ParseError};

pub fn date_parse(date_str: &str) -> Result<NaiveDateTime, ParseError> {
    // "%Y-%m-%dT%H:%M:%S.%fZ"
    NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_parameter() {
        let date_str = "2022-01-01T01:02:03.123Z";
        assert_eq!(
            date_parse(date_str).unwrap().to_string(),
            "2022-01-01 01:02:03.000000123"
        )
    }
}
