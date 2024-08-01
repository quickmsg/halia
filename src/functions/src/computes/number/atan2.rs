use anyhow::Result;
use message::Message;
use types::TargetValue;

use crate::computes::Computer;

struct Atan2 {
    field: String,
    arg: TargetValue,
    target_field: String,
}

pub fn new(field: String, target_field: String, arg: TargetValue) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Atan2 {
        field,
        arg,
        target_field,
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
                message::MessageValue::Int64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64((*mv as f64).atan2(arg)),
                ),
                message::MessageValue::Uint64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64((*mv as f64).atan2(arg)),
                ),
                message::MessageValue::Float64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64(mv.atan2(arg)),
                ),
                _ => {}
            },
            None => {}
        }
    }
}
