use crate::{add_or_set_message_value, computes::Computer, StringArg};
use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::computer::ItemConf;

use super::get_array_string_arg;

struct Concat {
    field: String,
    args: Vec<StringArg>,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    let args = get_array_string_arg(&conf, "value")?;

    Ok(Box::new(Concat {
        field: conf.field,
        target_field: conf.target_field,
        args,
    }))
}

impl Computer for Concat {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::String(s) => s,
                _ => return,
            },
            None => return,
        };

        let mut result = value.clone();

        for arg in &self.args {
            match arg {
                StringArg::Const(s) => result.push_str(s),
                StringArg::Field(field) => match message.get(field) {
                    Some(mv) => match mv {
                        MessageValue::String(s) => result.push_str(s),
                        _ => return,
                    },
                    None => return,
                },
            }
        }

        let result = MessageValue::String(result);
        add_or_set_message_value!(self, message, result);
    }
}
