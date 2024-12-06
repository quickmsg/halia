use anyhow::Result;
use message::MessageValue;

use crate::{add_or_set_message_value, computes::Computer, Args};

pub struct Round {
    field: String,
    target_field: Option<String>,
}

pub fn validate_conf(mut args: Args) -> Result<()> {
    args.validate_field_and_option_target_field()?;
    Ok(())
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Round {
        field,
        target_field,
    }))
}

impl Computer for Round {
    fn compute(&mut self, message: &mut message::Message) {
        let result = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => MessageValue::Int64(*mv),
                MessageValue::Float64(mv) => MessageValue::Float64(mv.round()),
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        add_or_set_message_value!(self, message, result);
    }
}
