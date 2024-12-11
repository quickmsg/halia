use anyhow::Result;
use message::MessageValue;

use crate::{
    add_or_set_message_value,
    nodes::{
        args::{Args, FloatFieldArg},
        computes::Computer,
    },
};

struct Log {
    field: String,
    target_field: Option<String>,
    base: FloatFieldArg,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    let base = args.take_float_field("base")?;
    Ok(Box::new(Log {
        field,
        target_field,
        base,
    }))
}

impl Computer for Log {
    fn compute(&mut self, message: &mut message::Message) {
        let base = match &self.base {
            FloatFieldArg::ConstFloat(f) => *f,
            FloatFieldArg::Field(f) => match message.get(f) {
                Some(mv) => match mv {
                    MessageValue::Int64(mv) => *mv as f64,
                    MessageValue::Float64(mv) => *mv,
                    _ => return,
                },
                None => return,
            },
        };

        let result = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => {
                    if *mv <= 0 {
                        MessageValue::Null
                    } else {
                        MessageValue::Float64((*mv as f64).log(base))
                    }
                }
                MessageValue::Float64(mv) => {
                    if *mv <= 0.0 {
                        MessageValue::Null
                    } else {
                        MessageValue::Float64(mv.log(base))
                    }
                }
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        add_or_set_message_value!(self, message, result);
    }
}
