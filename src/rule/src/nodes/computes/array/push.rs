use anyhow::Result;

use crate::{
    add_or_set_message_value,
    nodes::{
        args::{Args, BoolStringIntFloatFieldArg},
        computes::Computer,
    },
};

struct Push {
    field: String,
    target_field: Option<String>,
    values: Vec<BoolStringIntFloatFieldArg>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    let values = args.take_array_bool_string_int_float_field("values")?;
    Ok(Box::new(Push {
        field,
        target_field,
        values,
    }))
}

impl Computer for Push {
    fn compute(&mut self, message: &mut message::Message) {
        let mut value = match message.get_array(&self.field) {
            Some(mv) => mv.clone(),
            None => return,
        };

        for v in &self.values {
            match v {
                BoolStringIntFloatFieldArg::ConstBool(b) => {
                    value.push(message::MessageValue::Boolean(*b))
                }
                BoolStringIntFloatFieldArg::ConstString(s) => {
                    value.push(message::MessageValue::String(s.clone()))
                }
                BoolStringIntFloatFieldArg::ConstInt(i) => {
                    value.push(message::MessageValue::Int64(*i))
                }
                BoolStringIntFloatFieldArg::ConstFloat(f) => {
                    value.push(message::MessageValue::Float64(*f))
                }
                BoolStringIntFloatFieldArg::Field(f) => match message.get(f) {
                    Some(mv) => value.push(mv.clone()),
                    None => return,
                },
            }
        }

        add_or_set_message_value!(self, message, message::MessageValue::Array(value));
    }
}
