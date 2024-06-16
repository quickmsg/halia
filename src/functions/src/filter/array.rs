use super::{Filter, Rule};
use anyhow::{bail, Result};
use message::Message;
use serde_json::Value;
use tracing::debug;

pub(crate) fn get_array_filter(rule: Rule) -> Result<Box<dyn Filter>> {
    match rule.option.as_str() {
        "eq" => match rule.value {
            Some(value) => ArrayEq::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        "ne" => match rule.value {
            Some(value) => ArrayNe::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        "ct" => match rule.value {
            Some(value) => ArrayContains::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        _ => bail!("not support"),
    }
}

pub struct ArrayEq {
    field: String,
    value: Vec<Value>,
}

impl ArrayEq {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value {
            Value::Array(value) => Ok(Box::new(ArrayEq { field, value })),
            _ => bail!("Args must be array."),
        }
    }
}

impl Filter for ArrayEq {
    fn filter(&self, message: &Message) -> bool {
        match message.get_array(&self.field) {
            Some(message_value) => {
                debug!(
                    "message_value:{:?},conf value:{:?}",
                    message_value, self.value
                );
                if *message_value == self.value {
                    true
                } else {
                    false
                }
            }
            None => false,
        }
    }
}

pub struct ArrayNe {
    field: String,
    value: Vec<Value>,
}

impl ArrayNe {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value {
            Value::Array(value) => Ok(Box::new(ArrayNe { field, value })),
            _ => bail!("Args must be array."),
        }
    }
}

impl Filter for ArrayNe {
    fn filter(&self, message: &Message) -> bool {
        match message.get_array(&self.field) {
            Some(message_value) => {
                if *message_value != self.value {
                    true
                } else {
                    false
                }
            }
            None => false,
        }
    }
}

pub struct ArrayContains {
    field: String,
    values: Vec<Value>,
}

impl ArrayContains {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value {
            Value::Array(values) => Ok(Box::new(ArrayContains { field, values })),
            _ => bail!("Args must be array."),
        }
    }
}

impl Filter for ArrayContains {
    fn filter(&self, message: &Message) -> bool {
        match message.get_array(&self.field) {
            Some(message_value) => {
                for value in &self.values {
                    if !message_value.contains(value) {
                        return false;
                    }
                }
                true
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eq() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;
        let message = Message::from_str(data).unwrap();

        let value = r#"["+44 1234567", "+44 2345678"]"#;
        let value: Value = serde_json::from_str(value).unwrap();
        let array_eq = ArrayEq::new("phones".to_string(), value).unwrap();
        assert_eq!(array_eq.filter(&message), true);

        let value = r#"["+44 1234567"]"#;
        let value: Value = serde_json::from_str(value).unwrap();
        let array_eq = ArrayEq::new("phones".to_string(), value).unwrap();
        assert_eq!(array_eq.filter(&message), false);
    }

    #[test]
    fn ne() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;
        let message = Message::from_str(data).unwrap();

        let value = r#"["+44 1234567", "+44 2345678"]"#;
        let value: Value = serde_json::from_str(value).unwrap();
        let array_ne = ArrayNe::new("phones".to_string(), value).unwrap();
        assert_eq!(array_ne.filter(&message), false);

        let value = r#"["+44 1234567"]"#;
        let value: Value = serde_json::from_str(value).unwrap();
        let array_ne = ArrayNe::new("phones".to_string(), value).unwrap();
        assert_eq!(array_ne.filter(&message), true);
    }

    #[test]
    fn contains() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;
        let message = Message::from_str(data).unwrap();

        let value = r#"["+44 1234567", "+44 2345678"]"#;
        let value: Value = serde_json::from_str(value).unwrap();
        let array_co = ArrayContains::new("phones".to_string(), value).unwrap();
        assert_eq!(array_co.filter(&message), true);

        let value = r#"["+44 1234567"]"#;
        let value: Value = serde_json::from_str(value).unwrap();
        let array_co = ArrayContains::new("phones".to_string(), value).unwrap();
        assert_eq!(array_co.filter(&message), true);

        let value = r#"["+44 1234567", "+44 2345678", "5523"]"#;
        let value: Value = serde_json::from_str(value).unwrap();
        let array_co = ArrayContains::new("phones".to_string(), value).unwrap();
        assert_eq!(array_co.filter(&message), false);
    }
}
