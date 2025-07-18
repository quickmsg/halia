use anyhow::{bail, Result};
use async_trait::async_trait;
use common::get_dynamic_value_from_json;
use message::MessageValue;
use regex::Regex;
use tracing::warn;
use types::rules::functions::filter::ItemConf;

use super::Filter;

struct Reg {
    field: String,
    reg: Option<Regex>,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Filter>> {
    match get_dynamic_value_from_json(&conf.value) {
        common::DynamicValue::Const(value) => match value {
            serde_json::Value::String(s) => match Regex::new(&s) {
                Ok(reg) => Ok(Box::new(Reg {
                    field: conf.field,
                    reg: Some(reg),
                    target_field: None,
                })),
                Err(e) => bail!("regex err:{}", e),
            },
            _ => bail!("不支持该类型"),
        },
        common::DynamicValue::Field(s) => Ok(Box::new(Reg {
            field: conf.field,
            reg: None,
            target_field: Some(s),
        })),
    }
}

#[async_trait]
impl Filter for Reg {
    async fn filter(&self, message: &message::Message) -> bool {
        match (&self.reg, &self.target_field) {
            (Some(reg), None) => match message.get(&self.field) {
                Some(value) => match value {
                    MessageValue::String(string) => reg.is_match(string),
                    _ => false,
                },
                None => false,
            },
            (None, Some(target_field)) => {
                match (message.get(&self.field), message.get(target_field)) {
                    (Some(value), Some(target_value)) => match (value, target_value) {
                        (MessageValue::String(string), MessageValue::String(target_string)) => {
                            match Regex::new(target_string) {
                                Ok(reg) => reg.is_match(string),
                                Err(e) => {
                                    warn!("regex err:{}", e);
                                    false
                                }
                            }
                        }
                        _ => false,
                    },
                    _ => false,
                }
            }
            _ => unreachable!(),
        }
    }
}
