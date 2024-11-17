use crate::computes::{Arg, Computer};
use anyhow::{bail, Result};
use common::get_dynamic_value_from_json;
use message::{Message, MessageValue};
use types::rules::functions::computer::ItemConf;

struct Lpad {
    field: String,
    target_field: Option<String>,
    arg: StringArg,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    let (const_value, dynamic_value) = match conf.args {
        Some(mut args) => match args.pop() {
            Some(arg) => match get_dynamic_value_from_json(&arg) {
                common::DynamicValue::Const(value) => match value {
                    serde_json::Value::Number(number) => match number.as_i64() {
                        Some(u) => (Some(u), None),
                        None => bail!("只支持u64数字常量"),
                    },
                    _ => bail!("只支持数字常量"),
                },
                common::DynamicValue::Field(s) => (None, Some(s)),
            },
            None => bail!("Endswith function requires arguments"),
        },
        None => bail!("Endswith function requires arguments"),
    };
    Ok(Box::new(Lpad {
        field: conf.field,
        const_value,
        dynamic_value,
        target_field: conf.target_field,
    }))
}

impl Computer for Lpad {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::String(s) => s,
                _ => return,
            },
            None => return,
        };

        let target_value = match (&self.const_value, &self.dynamic_value) {
            (Some(value), None) => value,
            (None, Some(field)) => match message.get(field) {
                Some(mv) => match mv {
                    MessageValue::Int64(i) => i,
                    _ => return,
                },
                None => return,
            },
            _ => unreachable!(),
        };

        if *target_value < 0 {
            return;
        }

        // TOOD
        let resp_value = MessageValue::String(format!(
            "{: >width$}",
            value,
            width = *target_value as usize
        ));
        match &self.target_field {
            Some(target_field) => message.add(target_field.clone(), resp_value),
            None => message.set(&self.field, resp_value),
        }
    }
}
