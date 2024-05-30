use super::{Filter, Rule};
use anyhow::{bail, Result};
use message::Message;
use serde_json::Value;

pub(crate) fn get_int_filter(rule: Rule) -> Result<Box<dyn Filter>> {
    match rule.option.as_str() {
        "eq" => match rule.value {
            Some(value) => IntEq::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        "ne" => match rule.value {
            Some(value) => IntNe::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        "gt" => match rule.value {
            Some(value) => IntGt::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        "ge" => match rule.value {
            Some(value) => IntGe::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        "lt" => match rule.value {
            Some(value) => IntLt::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        "le" => match rule.value {
            Some(value) => IntLe::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        "in" => match rule.values {
            Some(values) => IntIn::new(rule.field.clone(), values),
            None => bail!("Must provide value"),
        },
        _ => bail!("not support"),
    }
}

pub struct IntEq {
    field: String,
    value: i64,
}

impl IntEq {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value {
            Value::Number(number) => {
                if let Some(i64) = number.as_i64() {
                    Ok(Box::new(IntEq { field, value: i64 }))
                } else {
                    bail!("Args must be int")
                }
            }
            _ => bail!("Args must be int"),
        }
    }
}

impl Filter for IntEq {
    fn filter(&self, message: &Message) -> bool {
        match message.get_i64(&self.field) {
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

pub struct IntNe {
    field: String,
    value: i64,
}

impl IntNe {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value {
            Value::Number(number) => {
                if let Some(i64) = number.as_i64() {
                    Ok(Box::new(IntNe { field, value: i64 }))
                } else {
                    bail!("Args must be int")
                }
            }
            _ => bail!("Args must be int"),
        }
    }
}

impl Filter for IntNe {
    fn filter(&self, message: &Message) -> bool {
        match message.get_i64(&self.field) {
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

pub struct IntGt {
    field: String,
    value: i64,
}

impl IntGt {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value {
            Value::Number(number) => {
                if let Some(i64) = number.as_i64() {
                    Ok(Box::new(IntGt { field, value: i64 }))
                } else {
                    bail!("Args must be int")
                }
            }
            _ => bail!("Args must be int"),
        }
    }
}

impl Filter for IntGt {
    fn filter(&self, message: &Message) -> bool {
        match message.get_i64(&self.field) {
            Some(message_value) => {
                if message_value > self.value {
                    true
                } else {
                    false
                }
            }
            None => false,
        }
    }
}

pub struct IntGe {
    field: String,
    value: i64,
}

impl IntGe {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value {
            Value::Number(number) => {
                if let Some(i64) = number.as_i64() {
                    Ok(Box::new(IntGe { field, value: i64 }))
                } else {
                    bail!("Args must be int")
                }
            }
            _ => bail!("Args must be int"),
        }
    }
}

impl Filter for IntGe {
    fn filter(&self, message: &Message) -> bool {
        match message.get_i64(&self.field) {
            Some(message_value) => {
                if message_value >= self.value {
                    true
                } else {
                    false
                }
            }
            None => false,
        }
    }
}

pub struct IntLt {
    field: String,
    value: i64,
}

impl IntLt {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value {
            Value::Number(number) => {
                if let Some(i64) = number.as_i64() {
                    Ok(Box::new(IntLt { field, value: i64 }))
                } else {
                    bail!("Args must be int")
                }
            }
            _ => bail!("Args must be int"),
        }
    }
}

impl Filter for IntLt {
    fn filter(&self, message: &Message) -> bool {
        match message.get_i64(&self.field) {
            Some(message_value) => {
                if message_value < self.value {
                    true
                } else {
                    false
                }
            }
            None => false,
        }
    }
}

pub struct IntLe {
    field: String,
    value: i64,
}

impl IntLe {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value {
            Value::Number(number) => {
                if let Some(i64) = number.as_i64() {
                    Ok(Box::new(IntLe { field, value: i64 }))
                } else {
                    bail!("Args must be int")
                }
            }
            _ => bail!("Args must be int"),
        }
    }
}

impl Filter for IntLe {
    fn filter(&self, message: &Message) -> bool {
        match message.get_i64(&self.field) {
            Some(message_value) => {
                if message_value <= self.value {
                    true
                } else {
                    false
                }
            }
            None => false,
        }
    }
}

pub struct IntIn {
    field: String,
    values: Vec<i64>,
}

impl IntIn {
    pub fn new(field: String, values: Vec<Value>) -> Result<Box<dyn Filter>> {
        let mut i64_values = Vec::new();
        for value in values {
            match value {
                Value::Number(i64) => {
                    if let Some(i64) = i64.as_i64() {
                        i64_values.push(i64);
                    }
                }
                _ => bail!("Args must is all int"),
            }
        }
        Ok(Box::new(IntIn {
            field,
            values: i64_values,
        }))
    }
}

impl Filter for IntIn {
    fn filter(&self, message: &Message) -> bool {
        match message.get_i64(&self.field) {
            Some(message_value) => {
                if self.values.contains(&message_value) {
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
            "test": {
                "a": 45,
                "b": {
                    "c": 42
                }
            },
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;
        let message = Message::from_str(data).unwrap();

        let value = r#"43"#;
        let value: Value = serde_json::from_str(value).unwrap();
        let int_eq = IntEq::new("age".to_string(), value).unwrap();
        assert_eq!(int_eq.filter(&message), true);

        let value = r#"42"#;
        let value: Value = serde_json::from_str(value).unwrap();
        let int_eq = IntEq::new("age".to_string(), value).unwrap();
        assert_eq!(int_eq.filter(&message), false);

        let value = r#"42"#;
        let value: Value = serde_json::from_str(value).unwrap();
        let int_eq = IntEq::new("test.b.c".to_string(), value).unwrap();
        assert_eq!(int_eq.filter(&message), true);
    }

    // #[test]
    // fn ne() {
    //     let data = r#"
    //     {
    //         "name": "John Doe",
    //         "age": 43,
    //         "phones": [
    //             "+44 1234567",
    //             "+44 2345678"
    //         ]
    //     }"#;
    //     let message = Message::from_str(data).unwrap();

    //     let value = r#"["+44 1234567", "+44 2345678"]"#;
    //     let value: Value = serde_json::from_str(value).unwrap();
    //     let array_ne = ArrayNe::new("/phones".to_string(), value).unwrap();
    //     assert_eq!(array_ne.filter(&message), false);

    //     let value = r#"["+44 1234567"]"#;
    //     let value: Value = serde_json::from_str(value).unwrap();
    //     let array_ne = ArrayNe::new("/phones".to_string(), value).unwrap();
    //     assert_eq!(array_ne.filter(&message), true);
    // }

    // #[test]
    // fn contains() {
    //     let data = r#"
    //     {
    //         "name": "John Doe",
    //         "age": 43,
    //         "phones": [
    //             "+44 1234567",
    //             "+44 2345678"
    //         ]
    //     }"#;
    //     let message = Message::from_str(data).unwrap();

    //     let value = r#"["+44 1234567", "+44 2345678"]"#;
    //     let value: Value = serde_json::from_str(value).unwrap();
    //     let array_co = ArrayContains::new("/phones".to_string(), value).unwrap();
    //     assert_eq!(array_co.filter(&message), true);

    //     let value = r#"["+44 1234567"]"#;
    //     let value: Value = serde_json::from_str(value).unwrap();
    //     let array_co = ArrayContains::new("/phones".to_string(), value).unwrap();
    //     assert_eq!(array_co.filter(&message), true);

    //     let value = r#"["+44 1234567", "+44 2345678", "5523"]"#;
    //     let value: Value = serde_json::from_str(value).unwrap();
    //     let array_co = ArrayContains::new("/phones".to_string(), value).unwrap();
    //     assert_eq!(array_co.filter(&message), false);
    // }
}
