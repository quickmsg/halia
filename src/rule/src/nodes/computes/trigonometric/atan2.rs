use anyhow::{bail, Result};
use message::{Message, MessageValue};
use types::{rules::functions::ComputerConfItem, TargetValue};

use super::Computer;

struct Atan2 {
    field: String,
    arg: TargetValue,
    target_field: Option<String>,
}

pub fn new(conf: ComputerConfItem) -> Result<Box<dyn Computer>> {
    let arg = match conf.arg {
        Some(mut arg) => match arg.pop() {
            Some(arg) => arg,
            None => bail!("必须拥有一个参数"),
        },
        None => bail!("atan2函数必须拥有参数"),
    };
    Ok(Box::new(Atan2 {
        field: conf.field,
        arg,
        target_field: conf.target_field,
    }))
}

impl Computer for Atan2 {
    fn compute(&self, message: &mut Message) {
        let arg = match self.arg.typ {
            types::TargetValueType::Const => match &self.arg.value {
                serde_json::Value::Number(n) => match n.as_f64() {
                    Some(value) => value,
                    None => return,
                },
                _ => return,
            },
            types::TargetValueType::Variable => match &self.arg.value {
                serde_json::Value::String(field) => match message.get(field) {
                    Some(mv) => match mv {
                        MessageValue::Int64(mv) => *mv as f64,
                        MessageValue::Float64(mv) => *mv,
                        _ => return,
                    },
                    None => return,
                },
                _ => return,
            },
        };

        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => MessageValue::Float64((*mv as f64).atan2(arg)),
                MessageValue::Float64(mv) => MessageValue::Float64(mv.atan2(arg)),
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        match &self.target_field {
            Some(target_field) => message.add(target_field.clone(), value),
            None => message.set(&self.field, value),
        }
    }
}
