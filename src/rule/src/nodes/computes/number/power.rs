use anyhow::Result;
use types::TargetValue;

use crate::computes::Computer;

pub struct Pow {
    field: String,
    arg: TargetValue,
    target_field: String,
}

impl Pow {
    pub fn new(field: String, arg: TargetValue, target_field: String) -> Result<Pow> {
        Ok(Pow {
            field,
            arg,
            target_field,
        })
    }
}

impl Computer for Pow {
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
                        message::MessageValue::Uint64(mv) => *mv as f64,
                        message::MessageValue::Float64(mv) => *mv,
                        _ => return,
                    },
                    None => return,
                },
                _ => return,
            },
        };

        match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::Int64(_) => todo!(),
                message::MessageValue::Uint64(_) => todo!(),
                message::MessageValue::Float64(mv) => mv.powf(n),
                _ => {}
            },
            None => {}
        }
    }
}
