use anyhow::Result;
use message::{Message, MessageValue};

use crate::{add_or_set_message_value, computes::Computer, Args};

static COUNT_KEY: &str = "count";

struct Repeat {
    field: String,
    target_field: Option<String>,
    count: usize,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    let count = args.take_usize(COUNT_KEY)?;
    Ok(Box::new(Repeat {
        field,
        target_field,
        count,
    }))
}

impl Computer for Repeat {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get_str(&self.field) {
            Some(mv) => mv,
            None => return,
        };

        let result = value.repeat(self.count);
        add_or_set_message_value!(self, message, MessageValue::String(result));
    }
}
