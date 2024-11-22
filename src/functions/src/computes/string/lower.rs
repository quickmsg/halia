use anyhow::Result;
use message::{Message, MessageValue};

use crate::{add_or_set_message_value, computes::Computer, Args};

struct Lower {
    field: String,
    target_field: Option<String>,
}

pub fn validate_conf(mut args: Args) -> Result<()> {
    args.validate_field_and_option_target_field()?;
    Ok(())
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Lower {
        field,
        target_field,
    }))
}

impl Computer for Lower {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get_str(&self.field) {
            Some(mv) => mv,
            None => return,
        };

        let result = value.to_lowercase();

        add_or_set_message_value!(self, message, MessageValue::String(result));
    }
}
