use crate::{computes::Computer, get_string_field_arg, StringFieldArg};
use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::ItemConf;

struct IndexOf {
    field: String,
    target_field: Option<String>,
    arg: StringFieldArg,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    let arg = get_string_field_arg(&conf, "value")?;
    Ok(Box::new(IndexOf {
        field: conf.field,
        target_field: conf.target_field,
        arg,
    }))
}

impl Computer for IndexOf {
    fn compute(&self, message: &mut Message) {
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
