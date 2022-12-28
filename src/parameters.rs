use chrono::NaiveDateTime;

/// Dune supports 4 different parameter types enumerated here:
/// In end, all parameters are passed to
/// Dune via the API as JSON strings.
#[derive(Debug, PartialEq)]
enum ParameterType {
    /// A.k.a. string (used for transaction hashes and evm addresses, etc.)
    Text,
    /// Encapsulates all numerical types (integer and float).
    Number,
    /// A.k.a List or Dropdown of text.
    Enum,
    /// Dune Date strings take the form `YYYY-MM-DD hh:mm:ss`
    Date,
}

#[derive(Debug, PartialEq)]
pub struct Parameter {
    /// Parameter Name.
    pub key: String,
    /// Currently unused type field
    /// (will become relevant when API supports `upsert_query`)
    ptype: ParameterType,
    /// String representation of parameter's value
    pub value: String,
}

impl Parameter {
    /// Constructor of Date type Parameter
    pub fn date(name: &str, value: NaiveDateTime) -> Self {
        Parameter {
            key: String::from(name),
            ptype: ParameterType::Date,
            // Dune date precision is to the second.
            // YYYY-MM-DD HH:MM:SS
            value: value.to_string()[..19].parse().unwrap(),
        }
    }

    /// Constructor of Text type Parameter
    pub fn text(name: &str, value: &str) -> Self {
        Parameter {
            key: String::from(name),
            ptype: ParameterType::Text,
            value: String::from(value),
        }
    }

    /// Constructor of Numeric type Parameter
    pub fn number(name: &str, value: &str) -> Self {
        Parameter {
            key: String::from(name),
            ptype: ParameterType::Number,
            value: String::from(value),
        }
    }

    /// Constructor of List/Enum type Parameter
    pub fn list(name: &str, value: &str) -> Self {
        Parameter {
            key: String::from(name),
            ptype: ParameterType::Enum,
            value: String::from(value),
        }
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

    #[test]
    fn derived_debug() {
        assert_eq!(format!("{:?}", ParameterType::Date), "Date");
        assert_eq!(
            format!("{:?}", Parameter::number("MyNumber", "3.14159")),
            "Parameter { key: \"MyNumber\", ptype: Number, value: \"3.14159\" }"
        );
    }
}
