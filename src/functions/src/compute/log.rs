use anyhow::{bail, Result};
use message::MessageValue;
use types::{rules::functions::ComputerConf, TargetValue};

use super::Computer;

struct Log {
    field: String,
    arg: TargetValue,
    target_field: Option<String>,
}

pub fn new(conf: ComputerConf) -> Result<Box<dyn Computer>> {
    let arg = match conf.arg {
        Some(arg) => arg,
        None => bail!("log必须含有参数"),
    };
    Ok(Box::new(Log {
        field: conf.field,
        arg,
        target_field: conf.target_field,
    }))
}

impl Computer for Log {
    fn compute(&self, message: &mut message::Message) {
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
                        message::MessageValue::Int64(mv) => *mv as f64,
                        message::MessageValue::Float64(mv) => *mv,
                        _ => return,
                    },
                    None => return,
                },
                _ => return,
            },
        };

        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => {
                    if *mv <= 0 {
                        MessageValue::Null
                    } else {
                        MessageValue::Float64((*mv as f64).log(arg))
                    }
                }
                MessageValue::Float64(mv) => {
                    if *mv <= 0.0 {
                        MessageValue::Null
                    } else {
                        MessageValue::Float64(mv.log(arg))
                    }
                }
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
