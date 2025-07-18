use anyhow::Result;
use message::MessageValue;

use crate::{
    add_or_set_message_value,
    nodes::{
        args::{Args, BoolStringIntFloatFieldArg},
        computes::Computer,
    },
};

struct IndexOf {
    field: String,
    target_field: Option<String>,
    search_element: BoolStringIntFloatFieldArg,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    let search_element = args.take_bool_string_int_float_field("search_element")?;
    Ok(Box::new(IndexOf {
        field,
        target_field,
        search_element,
    }))
}

impl Computer for IndexOf {
    fn compute(&mut self, message: &mut message::Message) {
        let arr = match message.get_array(&self.field) {
            Some(arr) => arr,
            None => return,
        };

        let result = arr.iter().position(|x| match (x, &self.search_element) {
            (MessageValue::Boolean(a), BoolStringIntFloatFieldArg::ConstBool(b)) => a == b,
            (MessageValue::String(a), BoolStringIntFloatFieldArg::ConstString(b)) => a == b,
            (MessageValue::Int64(a), BoolStringIntFloatFieldArg::ConstInt(b)) => a == b,
            (MessageValue::Float64(a), BoolStringIntFloatFieldArg::ConstFloat(b)) => a == b,
            _ => false,
        });
        let result = match result {
            Some(v) => v as i64,
            None => -1,
        };

        add_or_set_message_value!(self, message, MessageValue::Int64(result));
    }
}
