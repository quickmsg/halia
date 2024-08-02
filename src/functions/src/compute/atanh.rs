use anyhow::Result;
use message::Message;

struct Atanh {
    field: String,
    target_field: String,
}

pub fn new(field: String, target_field: String) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Atanh {
        field,
        target_field,
    }))
}

impl Computer for Atanh {
    fn compute(&self, message: &mut Message) {
        match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::Int64(mv) => {
                    if *mv <= -1 || *mv >= 1 {
                        return;
                    }
                    message.add(
                        self.target_field.clone(),
                        message::MessageValue::Float64((*mv as f64).atanh()),
                    )
                }
                message::MessageValue::Uint64(mv) => {
                    if *mv >= 1 {
                        return;
                    }
                    message.add(
                        self.target_field.clone(),
                        message::MessageValue::Float64((*mv as f64).atanh()),
                    )
                }
                message::MessageValue::Float64(mv) => {
                    if *mv <= -1.0 || *mv >= 1.0 {
                        return;
                    }
                    message.add(
                        self.target_field.clone(),
                        message::MessageValue::Float64(mv.atanh()),
                    )
                }
                _ => {}
            },
            None => {}
        }
    }
}
