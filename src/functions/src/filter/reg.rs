use anyhow::{bail, Result};
use message::MessageValue;
use regex::Regex;
use serde::Deserialize;

use super::Filter;

pub struct Reg {
    field: String,
    value: Regex,
}

#[derive(Deserialize)]
struct Conf {
    field: String,
    value: String,
}

impl Reg {
    pub fn new(conf: serde_json::Value) -> Result<Self> {
        let conf: Conf = serde_json::from_value(conf)?;
        match Regex::new(&conf.value) {
            Ok(reg) => Ok(Reg {
                field: conf.field,
                value: reg,
            }),
            Err(e) => bail!("regex err:{}", e),
        }
    }
}

impl Filter for Reg {
    fn filter(&self, message: &message::Message) -> bool {
        match message.get(&self.field) {
            Some(value) => match value {
                MessageValue::String(string) => {
                    if self.value.is_match(string) {
                        true
                    } else {
                        false
                    }
                }
                _ => false,
            },
            None => false,
        }
    }
}
