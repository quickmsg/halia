use anyhow::Result;

use crate::{
    add_or_set_message_value,
    nodes::{
        args::{Args, IntFloatFieldArg},
        computes::Computer,
    },
};

pub struct Pow {
    field: String,
    target_field: Option<String>,
    n: IntFloatFieldArg,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    let n = args.take_int_float_field("n")?;
    Ok(Box::new(Pow {
        field,
        target_field,
        n,
    }))
}

enum N {
    Int(i64),
    Float(f64),
}

impl Computer for Pow {
    fn compute(&mut self, message: &mut message::Message) {
        let n = match &self.n {
            IntFloatFieldArg::ConstInt(i) => N::Int(*i),
            IntFloatFieldArg::ConstFloat(f) => N::Float(*f),
            IntFloatFieldArg::Field(f) => match message.get(f) {
                Some(mv) => match mv {
                    message::MessageValue::Int64(i) => N::Int(*i),
                    message::MessageValue::Float64(f) => N::Float(*f),
                    _ => return,
                },
                None => return,
            },
        };

        let result = match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::Int64(i) => match n {
                    N::Int(n) => message::MessageValue::Int64(i.pow(n as u32)),
                    N::Float(n) => message::MessageValue::Float64((*i as f64).powf(n)),
                },
                message::MessageValue::Float64(mv) => match n {
                    N::Int(n) => message::MessageValue::Float64(mv.powi(n as i32)),
                    N::Float(n) => message::MessageValue::Float64(mv.powf(n)),
                },
                _ => return,
            },
            None => return,
        };

        add_or_set_message_value!(self, message, result);
    }
}
