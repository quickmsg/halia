use anyhow::Result;
use message::{Message, MessageValue};

use crate::{
    add_or_set_message_value,
    nodes::{args::Args, computes::Computer},
};

struct Length {
    field: String,
    target_field: Option<String>,
}

pub fn validate_conf(mut args: Args) -> Result<()> {
    args.validate_field_and_option_target_field()?;
    Ok(())
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Length {
        field,
        target_field,
    }))
}

impl Computer for Length {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get_str(&self.field) {
            Some(v) => v,
            None => return,
        };

        let reuslt = value.len() as i64;

        add_or_set_message_value!(self, message, MessageValue::Int64(reuslt));
    }
}
