use crate::{
    add_or_set_message_value,
    computes::{Arg, Computer},
};
use anyhow::{bail, Result};
use common::get_dynamic_value_from_json;
use message::{Message, MessageValue};
use types::rules::functions::computer::ItemConf;

struct Concat {
    field: String,
    args: Vec<Arg>,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    let args = conf
        .args
        .and_then(|mut conf_args| conf_args.remove("values"))
        .ok_or_else(|| anyhow::anyhow!("concat function requires values arguments"))?
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("values arguments must be an array"))?
        .iter()
        .map(|right_arg| match get_dynamic_value_from_json(right_arg) {
            common::DynamicValue::Const(value) => match value {
                serde_json::Value::String(s) => Ok(Arg::Const(s)),
                _ => bail!("invalid value"),
            },
            common::DynamicValue::Field(f) => Ok(Arg::Field(f)),
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Box::new(Concat {
        field: conf.field,
        args,
        target_field: conf.target_field,
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
                Arg::Const(s) => result.push_str(s),
                Arg::Field(field) => match message.get(field) {
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
