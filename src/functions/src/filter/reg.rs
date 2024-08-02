use anyhow::{bail, Result};
use message::MessageValue;
use regex::Regex;
use serde::Deserialize;
use types::rules::functions::FilterConf;

use super::Filter;

struct Reg {
    field: String,
    value: Regex,
}

pub const TYPE: &str = "reg";

#[derive(Deserialize)]
struct Conf {
    field: String,
    value: String,
}

pub fn new(conf: FilterConf) -> Result<Box<dyn Filter>> {
    match Regex::new(&conf.value) {
        Ok(reg) => Ok(Box::new(Reg {
            field: conf.field,
            value: reg,
        })),
        Err(e) => bail!("regex err:{}", e),
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
