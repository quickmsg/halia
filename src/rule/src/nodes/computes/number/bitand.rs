use anyhow::{bail, Result};
use common::get_dynamic_value_from_json;
use message::{Message, MessageValue};
use types::rules::functions::computer::ItemConf;

use crate::computes::Computer;

struct Bitand {
    field: String,
    const_arg: Option<i64>,
    target_value_field: Option<String>,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    let (const_arg, target_value_field) = match conf.args {
        Some(args) => {
            if args.len() != 1 {
                bail!("bitand function needs 1 argument");
            }
            match get_dynamic_value_from_json(&args[0]) {
                common::DynamicValue::Const(value) => match value {
                    serde_json::Value::Number(number) => match number.as_i64() {
                        Some(v) => (Some(v), None),
                        None => bail!("bitand function needs a const i64 value"),
                    },
                    _ => bail!("bitand function needs a const i64 value"),
                },
                common::DynamicValue::Field(s) => (None, Some(s)),
            }
        }
        None => bail!("bitand function needs a const value"),
    };

    Ok(Box::new(Bitand {
        field: conf.field,
        const_arg,
        target_value_field,
        target_field: conf.target_field,
    }))
}

impl Computer for Bitand {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => mv,
                _ => return,
            },
            None => return,
        };

        let target_value = match (&self.const_arg, &self.target_value_field) {
            (Some(const_value), None) => const_value,
            (None, Some(target_value_field)) => match message.get(&target_value_field) {
                Some(target_value) => match target_value {
                    MessageValue::Int64(v) => v,
                    _ => return,
                },
                None => return,
            },
            _ => unreachable!(),
        };

        let resp_value = MessageValue::Int64(value & target_value);

        match &self.target_field {
            Some(target_field) => message.add(target_field.clone(), resp_value),
            None => message.set(&self.field, resp_value),
        }
    }
}
