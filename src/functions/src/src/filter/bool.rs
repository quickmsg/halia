use super::{Filter, Rule};
use anyhow::{bail, Result};
use message::Message;
use serde_json::Value;

pub(crate) fn get_bool_filter(rule: Rule) -> Result<Box<dyn Filter>> {
    match rule.option.as_str() {
        "eq" => match rule.value {
            Some(value) => BoolEq::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        "ne" => match rule.value {
            Some(value) => BoolNe::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        _ => bail!("not support"),
    }
}

pub struct BoolEq {
    field: String,
    value: bool,
}

impl BoolEq {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value {
            Value::Bool(bool) => Ok(Box::new(BoolEq { field, value: bool })),
            _ => bail!("Args must be boolean"),
        }
    }
}

impl Filter for BoolEq {
    fn filter(&self, message: &Message) -> bool {
        match message.get_bool(&self.field) {
            Some(message_value) => {
                if message_value == self.value {
                    true
                } else {
                    false
                }
            }
            None => false,
        }
    }
}

pub struct BoolNe {
    field: String,
    value: bool,
}

impl BoolNe {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value {
            Value::Bool(bool) => Ok(Box::new(BoolNe { field, value: bool })),
            _ => bail!("Args must be boolean"),
        }
    }
}

impl Filter for BoolNe {
    fn filter(&self, message: &Message) -> bool {
        match message.get_bool(&self.field) {
            Some(message_value) => {
                if message_value != self.value {
                    true
                } else {
                    false
                }
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
            ],
            "a": true,
            "b": false
        }"#;
        let message = Message::from_str(data).unwrap();

        let value = r#"true"#;
        let value: Value = serde_json::from_str(value).unwrap();
        let bool_eq = BoolEq::new("a".to_string(), value).unwrap();
        assert_eq!(bool_eq.filter(&message), true);

        let value = r#"false"#;
        let value: Value = serde_json::from_str(value).unwrap();
        let bool_eq = BoolEq::new("b".to_string(), value).unwrap();
        assert_eq!(bool_eq.filter(&message), true);
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
            ],
            "a": true,
            "b": false
        }"#;
        let message = Message::from_str(data).unwrap();

        let value = r#"true"#;
        let value: Value = serde_json::from_str(value).unwrap();
        let bool_ne = BoolNe::new("a".to_string(), value).unwrap();
        assert_eq!(bool_ne.filter(&message), false);

        let value = r#"false"#;
        let value: Value = serde_json::from_str(value).unwrap();
        let bool_ne = BoolNe::new("b".to_string(), value).unwrap();
        assert_eq!(bool_ne.filter(&message), false);
    }
}