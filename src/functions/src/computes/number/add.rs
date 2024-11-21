use anyhow::Result;
use message::{Message, MessageValue};

use crate::{
    add_or_set_message_value, computes::Computer, get_array_int_float_field_arg, Args,
    IntFloatFieldArg,
};

struct Add {
    field: String,
    target_field: Option<String>,
    args: Vec<IntFloatFieldArg>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = crate::get_field_and_option_target_field(&mut args)?;
    let args = get_array_int_float_field_arg(&mut args, "value")?;

    Ok(Box::new(Add {
        field,
        target_field,
        args,
    }))
}

impl Computer for Add {
    fn compute(&mut self, message: &mut Message) {
        let mut result = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => MessageValue::Int64(*mv),
                MessageValue::Float64(mv) => MessageValue::Float64(*mv),
                _ => return,
            },
            None => return,
        };

        for arg in &self.args {
            match arg {
                IntFloatFieldArg::ConstInt(i) => match result {
                    MessageValue::Int64(v) => result = MessageValue::Int64(v + i),
                    MessageValue::Float64(v) => result = MessageValue::Float64(v + *i as f64),
                    _ => return,
                },
                IntFloatFieldArg::ConstFloat(f) => match result {
                    MessageValue::Int64(v) => result = MessageValue::Float64(v as f64 + f),
                    MessageValue::Float64(v) => result = MessageValue::Float64(v + f),
                    _ => return,
                },
                IntFloatFieldArg::Field(field) => match message.get(field) {
                    Some(mv) => match mv {
                        MessageValue::Int64(i) => match result {
                            MessageValue::Int64(v) => result = MessageValue::Int64(v + i),
                            MessageValue::Float64(v) => {
                                result = MessageValue::Float64(v + *i as f64)
                            }
                            _ => return,
                        },
                        MessageValue::Float64(f) => match result {
                            MessageValue::Int64(v) => result = MessageValue::Float64(v as f64 + f),
                            MessageValue::Float64(v) => result = MessageValue::Float64(v + f),
                            _ => return,
                        },
                        _ => return,
                    },
                    None => return,
                },
            }
        }

        add_or_set_message_value!(self, message, result);
    }
}
