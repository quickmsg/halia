use crate::{computes::Computer, StringArg};
use anyhow::Result;
use message::{Message, MessageValue};
use regex::Regex;
use types::rules::functions::computer::ItemConf;

use super::get_string_arg;

struct RegexMatch {
    field: String,
    target_field: Option<String>,
    arg: StringArg,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    let arg = get_string_arg(&conf, "value")?;

    Ok(Box::new(RegexMatch {
        field: conf.field,
        target_field: conf.target_field,
        arg,
    }))
}

impl Computer for RegexMatch {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::String(s) => s,
                _ => return,
            },
            None => return,
        };

        let resp_value = match &self.arg {
            StringArg::Const(s) => self.reg.find(&value).is_some(),
            StringArg::Field(s) => match message.get(s) {
                Some(mv) => match mv {
                    MessageValue::String(s) => match Regex::new(&s) {
                        Ok(reg) => reg.find(&value).is_some(),
                        Err(_) => return,
                    },
                    _ => return,
                },
                None => return,
            },
        };

        let resp_value = MessageValue::Boolean(resp_value);

        match &self.target_field {
            Some(target_field) => message.add(target_field.clone(), resp_value),
            None => message.set(&self.field, resp_value),
        }
    }
}
