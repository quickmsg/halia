use anyhow::Result;
use message::{Message, MessageValue};

use crate::{computes::Computer, get_string_field_arg, Args, StringFieldArg};

struct IndexOf {
    field: String,
    target_field: Option<String>,
    arg: StringFieldArg,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = crate::get_field_and_option_target_field(&mut args)?;
    let arg = get_string_field_arg(&mut args, "value")?;
    Ok(Box::new(IndexOf {
        field,
        target_field,
        arg,
    }))
}

impl Computer for IndexOf {
    fn compute(&mut self, message: &mut Message) {
        let value = message.get(&self.field).and_then(|mv| match mv {
            MessageValue::String(s) => Some(s),
            _ => None,
        });

        let value = match value {
            Some(s) => s,
            None => return,
        };

        let target_value = match &self.arg {
            StringFieldArg::Const(s) => s,
            StringFieldArg::Field(f) => match message.get(f) {
                Some(mv) => match mv {
                    MessageValue::String(s) => s,
                    _ => return,
                },
                None => return,
            },
        };

        let index = match value.find(target_value) {
            Some(p) => p as i64,
            None => -1,
        };
        let resp_value = MessageValue::Int64(index);
        match &self.target_field {
            Some(target_field) => message.add(target_field.clone(), resp_value),
            None => message.set(&self.field, resp_value),
        }
    }
}
