use anyhow::Result;

use crate::computes::Computer;

// 弧度转为角度
struct Degrees {
    field: String,
    target_field: String,
}

pub fn new(field: String, target_field: String) -> Result<Degrees> {
    Ok(Degrees {
        field,
        target_field,
    })
}

impl Computer for Degrees {
    fn compute(&self, message: &mut message::Message) {
        match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::Int64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64((*mv as f64).to_degrees()),
                ),
                message::MessageValue::Uint64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64((*mv as f64).to_degrees()),
                ),
                message::MessageValue::Float64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64(mv.to_degrees()),
                ),
                _ => {}
            },
            None => {}
        }
    }
}
