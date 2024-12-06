use anyhow::Result;
use message::{Message, MessageValue};
use regex::Regex;

use crate::{
    add_or_set_message_value,
    nodes::{
        args::{Args, StringFieldArg},
        computes::Computer,
    },
};

const REGEXP_KEY: &str = "regexp";

struct RegexMatch {
    field: String,
    target_field: Option<String>,
    regexp_const: Option<Regex>,
    regexp_field: Option<String>,
}

pub fn validate_conf(mut args: Args) -> Result<()> {
    args.validate_field_and_option_target_field()?;
    Ok(())
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    let regexp = args.take_string_field(REGEXP_KEY)?;
    let (regexp_const, regexp_field) = match regexp {
        StringFieldArg::Const(s) => match Regex::new(&s) {
            Ok(reg) => (Some(reg), None),
            Err(e) => return Err(anyhow::anyhow!(format!("Invalid regexp, err: {:?}", e))),
        },
        StringFieldArg::Field(f) => (None, Some(f)),
    };

    Ok(Box::new(RegexMatch {
        field,
        target_field,
        regexp_const,
        regexp_field,
    }))
}

impl Computer for RegexMatch {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get_str(&self.field) {
            Some(mv) => mv,
            None => return,
        };

        let resp_value = match (&self.regexp_const, &self.regexp_field) {
            (None, Some(field)) => match message.get_str(field) {
                Some(mv) => match Regex::new(&mv) {
                    Ok(reg) => reg.find(&value).is_some(),
                    Err(_) => return,
                },
                None => return,
            },
            (Some(regex), None) => regex.find(&value).is_some(),
            _ => unreachable!(),
        };

        let result = MessageValue::Boolean(resp_value);
        add_or_set_message_value!(self, message, result);
    }
}
