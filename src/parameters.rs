use chrono::NaiveDateTime;
use serde::Serialize;

#[derive(Serialize, Debug, PartialEq)]
pub enum ParameterType {
    Text,
    Number,
    Enum,
    Date,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct Parameter {
    pub key: String,
    // #[serde(rename(serialize = "type"))]
    #[serde(skip_serializing)]
    ptype: ParameterType,
    pub value: String,
}

impl Parameter {
    pub fn date(name: &str, value: NaiveDateTime) -> Self {
        Parameter {
            key: String::from(name),
            ptype: ParameterType::Date,
            // Dune date precision is to the second.
            // YYYY-MM-DD HH:MM:SS
            value: value.to_string()[..19].parse().unwrap(),
        }
    }

    pub fn text(name: &str, value: &str) -> Self {
        Parameter {
            key: String::from(name),
            ptype: ParameterType::Text,
            value: String::from(value),
        }
    }

    pub fn number(name: &str, value: &str) -> Self {
        Parameter {
            key: String::from(name),
            ptype: ParameterType::Number,
            value: String::from(value),
        }
    }

    pub fn list(name: &str, value: &str) -> Self {
        Parameter {
            key: String::from(name),
            ptype: ParameterType::Enum,
            value: String::from(value),
        }
    }

    pub fn to_dune(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::date_parse;

    #[test]
    fn new_parameter() {
        assert_eq!(
            Parameter::text("MyText", "Hello!"),
            Parameter {
                key: "MyText".to_string(),
                ptype: ParameterType::Text,
                value: "Hello!".to_string(),
            }
        );
        assert_eq!(
            Parameter::list("MyEnum", "Item 1"),
            Parameter {
                key: "MyEnum".to_string(),
                ptype: ParameterType::Enum,
                value: "Item 1".to_string(),
            }
        );
        assert_eq!(
            Parameter::number("MyNumber", "3.14159"),
            Parameter {
                key: "MyNumber".to_string(),
                ptype: ParameterType::Number,
                value: "3.14159".to_string(),
            }
        );
        let date_str = "2022-01-01T01:02:03.123Z";
        assert_eq!(
            Parameter::date("MyDate", date_parse(date_str).unwrap()),
            Parameter {
                key: "MyDate".to_string(),
                ptype: ParameterType::Date,
                value: "2022-01-01 01:02:03".to_string(),
            }
        )
    }
}
