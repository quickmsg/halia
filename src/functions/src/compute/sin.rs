use anyhow::Result;
use message::Message;

use crate::computes::Computer;

struct Sin {
    field: String,
    target_field: String,
}

fn new(field: String, target_field: String) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Sin {
        field,
        target_field,
    }))
}

impl Computer for Sin {
    fn compute(&self, message: &mut Message) {
        match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::Null => todo!(),
                message::MessageValue::Boolean(_) => todo!(),
                message::MessageValue::Int64(_) => todo!(),
                message::MessageValue::Uint64(_) => todo!(),
                message::MessageValue::Float64(_) => todo!(),
                message::MessageValue::String(_) => todo!(),
                message::MessageValue::Bytes(_) => todo!(),
                message::MessageValue::Array(_) => todo!(),
                message::MessageValue::Object(_) => todo!(),
            },
            None => todo!(),
        }
    }
}
