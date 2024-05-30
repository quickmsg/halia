use super::{Filter, Rule};
use anyhow::{bail, Result};
use message::Message;
use serde_json::Value;

pub(crate) fn get_float_filter(rule: Rule) -> Result<Box<dyn Filter>> {
    match rule.option.as_str() {
        "eq" => match rule.value {
            Some(value) => FloatEq::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        "ne" => match rule.value {
            Some(value) => FloatNe::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        "gt" => match rule.value {
            Some(value) => FloatGt::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        "ge" => match rule.value {
            Some(value) => FloatGe::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        "lt" => match rule.value {
            Some(value) => FloatLt::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        "le" => match rule.value {
            Some(value) => FloatLe::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        "in" => match rule.values {
            Some(values) => FloatIn::new(rule.field.clone(), values),
            None => bail!("Must provide value"),
        },
        _ => bail!("not support"),
    }
}


pub struct FloatEq {
    field: String,
    value: f64,
}

impl FloatEq {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value {
            Value::Number(number) => {
                if let Some(f64) = number.as_f64() {
                    Ok(Box::new(FloatEq { field, value: f64 }))
                } else {
                    bail!("Args must be float")
                }
            }
            _ => bail!("Args must be float"),
        }
    }
}

impl Filter for FloatEq {
    fn filter(&self, message: &Message) -> bool {
        match message.get_f64(&self.field) {
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

pub struct FloatNe {
    field: String,
    value: f64,
}

impl FloatNe {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value {
            Value::Number(number) => {
                if let Some(f64) = number.as_f64() {
                    Ok(Box::new(FloatNe { field, value: f64 }))
                } else {
                    bail!("Args must be int")
                }
            }
            _ => bail!("Args must be int"),
        }
    }
}

impl Filter for FloatNe {
    fn filter(&self, message: &Message) -> bool {
        match message.get_f64(&self.field) {
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

pub struct FloatGt {
    field: String,
    value: f64,
}

impl FloatGt {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value {
            Value::Number(number) => {
                if let Some(f64) = number.as_f64() {
                    Ok(Box::new(FloatGt { field, value: f64 }))
                } else {
                    bail!("Args must be int")
                }
            }
            _ => bail!("Args must be int"),
        }
    }
}

impl Filter for FloatGt {
    fn filter(&self, message: &Message) -> bool {
        match message.get_f64(&self.field) {
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

pub struct FloatGe {
    field: String,
    value: f64,
}

impl FloatGe {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value {
            Value::Number(number) => {
                if let Some(f64) = number.as_f64() {
                    Ok(Box::new(FloatGe { field, value: f64 }))
                } else {
                    bail!("Args must be int")
                }
            }
            _ => bail!("Args must be int"),
        }
    }
}

impl Filter for FloatGe {
    fn filter(&self, message: &Message) -> bool {
        match message.get_f64(&self.field) {
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

pub struct FloatLt {
    field: String,
    value: f64,
}

impl FloatLt {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value {
            Value::Number(number) => {
                if let Some(f64) = number.as_f64() {
                    Ok(Box::new(FloatLt { field, value: f64 }))
                } else {
                    bail!("Args must be int")
                }
            }
            _ => bail!("Args must be int"),
        }
    }
}

impl Filter for FloatLt {
    fn filter(&self, message: &Message) -> bool {
        match message.get_f64(&self.field) {
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

pub struct FloatLe {
    field: String,
    value: f64,
}

impl FloatLe {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value {
            Value::Number(number) => {
                if let Some(f64) = number.as_f64() {
                    Ok(Box::new(FloatLe { field, value: f64 }))
                } else {
                    bail!("Args must be float")
                }
            }
            _ => bail!("Args must be float"),
        }
    }
}

impl Filter for FloatLe {
    fn filter(&self, message: &Message) -> bool {
        match message.get_f64(&self.field) {
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

pub struct FloatIn {
    field: String,
    values: Vec<f64>,
}

impl FloatIn {
    pub fn new(field: String, values: Vec<Value>) -> Result<Box<dyn Filter>> {
        let mut f64_values = Vec::new();
        for value in values {
            match value {
                Value::Number(number) => {
                    if let Some(f64) = number.as_f64() {
                        f64_values.push(f64);
                    }
                }
                _ => bail!("Args must is all float"),
            }
        }
        Ok(Box::new(FloatIn {
            field,
            values: f64_values,
        }))
    }
}

impl Filter for FloatIn {
    fn filter(&self, message: &Message) -> bool {
        match message.get_f64(&self.field) {
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
