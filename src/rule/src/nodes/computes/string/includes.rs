use anyhow::Result;
use message::{Message, MessageValue};

use crate::{
    add_or_set_message_value,
    nodes::{
        args::{Args, StringFieldArg},
        computes::Computer,
    },
};

struct Includes {
    field: String,
    target_field: Option<String>,
    search_string: StringFieldArg,
}

pub fn validate_conf(mut args: Args) -> Result<()> {
    args.validate_field_and_option_target_field()?;
    Ok(())
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    let search_string = args.take_string_field("search_string")?;
    Ok(Box::new(Includes {
        field,
        target_field,
        search_string,
    }))
}

impl Computer for Includes {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get_str(&self.field) {
            Some(v) => v,
            None => return,
        };

        let search_string = match &self.search_string {
            StringFieldArg::Const(s) => s,
            StringFieldArg::Field(f) => match message.get_str(f) {
                Some(mv) => mv,
                None => return,
            },
        };

        let result = MessageValue::Boolean(value.contains(search_string));
        add_or_set_message_value!(self, message, result);
    }
}
