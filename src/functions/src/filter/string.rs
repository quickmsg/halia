use super::{Filter, Rule};
use anyhow::{bail, Result};
use message::Message;
use regex::Regex;
use serde_json::Value;

pub(crate) fn get_string_filter(rule: Rule) -> Result<Box<dyn Filter>> {
    match rule.option.as_str() {
        "eq" => match rule.value {
            Some(value) => StringEq::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        "ne" => match rule.value {
            Some(value) => StringNe::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        "regex" => match rule.value {
            Some(value) => StringReg::new(rule.field.clone(), value),
            None => bail!("Must provide value"),
        },
        _ => bail!("not support"),
    }
}

pub struct StringEq {
    field: String,
    value: String,
}

impl StringEq {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value {
            Value::String(value) => Ok(Box::new(StringEq { field, value })),
            _ => bail!("Args must be string"),
        }
    }
}

impl Filter for StringEq {
    fn filter(&self, message: &Message) -> bool {
        match message.get_string(&self.field) {
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

pub struct StringNe {
    field: String,
    value: String,
}

impl StringNe {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value {
            Value::String(value) => Ok(Box::new(StringNe { field, value })),
            _ => bail!("Args must be boolean"),
        }
    }
}

impl Filter for StringNe {
    fn filter(&self, message: &Message) -> bool {
        match message.get_string(&self.field) {
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

pub struct StringReg {
    field: String,
    value: Regex,
}

impl StringReg {
    pub fn new(field: String, value: Value) -> Result<Box<dyn Filter>> {
        match value.as_str() {
            Some(str) => match Regex::new(str) {
                Ok(value) => Ok(Box::new(StringReg { field, value })),
                Err(e) => bail!("regex err:{}", e),
            },
            None => bail!("Args must be string"),
        }
    }
}

impl Filter for StringReg {
    fn filter(&self, message: &message::Message) -> bool {
        match message.get_string(&self.field) {
            Some(message_value) => {
                if self.value.is_match(message_value) {
                    true
                } else {
                    false
                }
            }
            None => false,
        }
    }
}
