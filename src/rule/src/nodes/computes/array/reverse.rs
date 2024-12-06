use anyhow::Result;
use message::MessageValue;

use crate::{
    add_or_set_message_value,
    nodes::{args::Args, computes::Computer},
};

struct Reverse {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Reverse {
        field,
        target_field,
    }))
}

impl Computer for Reverse {
    fn compute(&mut self, message: &mut message::Message) {
        let arr = match message.get_array(&self.field) {
            Some(arr) => arr,
            None => return,
        };

        let mut result = arr.clone();
        result.reverse();

        add_or_set_message_value!(self, message, MessageValue::Array(result));
    }
}
