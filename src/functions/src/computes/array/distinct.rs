use anyhow::Result;
use message::MessageValue;

use crate::{add_or_set_message_value, args::Args, computes::Computer};

// 去重
pub struct Distinct {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Distinct {
        field,
        target_field,
    }))
}

impl Computer for Distinct {
    fn compute(&mut self, message: &mut message::Message) {
        let value = match message.get_array(&self.field) {
            Some(mv) => mv,
            None => return,
        };

        let mut result = Vec::new();
        for item in value.iter() {
            if !result.contains(item) {
                result.push(item.clone());
            }
        }

        add_or_set_message_value!(self, message, MessageValue::Array(result));
    }
}
