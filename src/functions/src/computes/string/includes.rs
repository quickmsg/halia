use crate::{
    add_or_set_message_value, computes::Computer, get_string_field_arg, Args, StringFieldArg,
};
use anyhow::Result;
use message::{Message, MessageValue};

static SEARCH_STRING_KEY: &str = "search_string";

struct Includes {
    field: String,
    target_field: Option<String>,
    search_string: StringFieldArg,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = crate::get_field_and_option_target_field(&mut args)?;
    let search_string = get_string_field_arg(&mut args, SEARCH_STRING_KEY)?;
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
