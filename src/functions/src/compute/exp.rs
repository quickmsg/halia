use anyhow::Result;
use message::MessageValue;
use types::rules::functions::ComputerConf;

use super::Computer;

pub struct Exp {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: ComputerConf) -> Result<Box<dyn Computer>> {
    Ok(Exp {
        field: conf.field,
        target_field: conf.target_field,
    })
}

impl Computer for Exp {
    fn compute(&self, message: &mut message::Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => MessageValue::Float64((*mv as f64).exp()),
                MessageValue::Uint64(mv) => MessageValue::Float64((*mv as f64).exp()),
                MessageValue::Float64(mv) => MessageValue::Float64(mv.exp()),
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
