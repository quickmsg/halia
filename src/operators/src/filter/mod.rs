use anyhow::{bail, Result};
use message::{Message, MessageBatch};
use serde::Deserialize;
use serde_json::Value;
use types::rule::Operate;

use self::{
    array::get_array_filter, bool::get_bool_filter, float::get_float_filter, int::get_int_filter,
    string::get_string_filter,
};

pub mod array;
pub mod bool;
pub mod exists;
pub mod float;
pub mod int;
pub mod not_exists;
pub mod string;

pub trait Filter: Sync + Send {
    fn filter(&self, message: &Message) -> bool;
}

#[derive(Deserialize)]
pub struct Rule {
    r#type: String,
    option: String,
    field: String,
    value: Option<Value>,
    values: Option<Vec<Value>>,
}

pub struct Node {
    filters: Vec<Box<dyn Filter>>,
}

impl Node {
    pub fn new(conf: Value) -> Result<Self> {
        let mut filters: Vec<Box<dyn Filter>> = Vec::new();
        let rules: Vec<Rule> = serde_json::from_value(conf)?;
        for rule in rules {
            match rule.r#type.as_str() {
                "int" => match get_int_filter(rule) {
                    Ok(filter) => filters.push(filter),
                    Err(e) => {
                        return Err(e);
                    }
                },
                "float" => match get_float_filter(rule) {
                    Ok(filter) => filters.push(filter),
                    Err(e) => {
                        return Err(e);
                    }
                },
                "bool" => match get_bool_filter(rule) {
                    Ok(filter) => filters.push(filter),
                    Err(e) => {
                        return Err(e);
                    }
                },
                "string" => match get_string_filter(rule) {
                    Ok(filter) => filters.push(filter),
                    Err(e) => {
                        return Err(e);
                    }
                },
                "array" => match get_array_filter(rule) {
                    Ok(filter) => filters.push(filter),
                    Err(e) => {
                        return Err(e);
                    }
                },
                "object" => {}
                _ => bail!("not support type"),
            }

            // "exists" => {
            //     filters.push(Exists::new(rule.field.clone()));
            // }

            // "not_exists" => {
            //     filters.push(NotExists::new(rule.field.clone()));
            // }

            // _ => bail!("not support option"),
        }

        Ok(Node { filters })
    }
}

impl Operate for Node {
    fn operate(&self, message_batch: &mut MessageBatch) -> bool {
        let messages = message_batch.get_messages_mut();
        messages.retain(|message| {
            for filter in &self.filters {
                if filter.filter(message) {
                    return true;
                }
            }

            false
        });

        true
    }
}
