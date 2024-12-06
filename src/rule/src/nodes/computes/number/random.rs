use anyhow::Result;
use message::MessageValue;
use rand::Rng;

use crate::nodes::{
    args::{Args, IntFloatFieldArg},
    computes::Computer,
};

pub struct Random {
    target_field: String,
    start: IntFloatFieldArg,
    end: IntFloatFieldArg,
}

pub fn validate_conf(mut args: Args) -> Result<()> {
    args.validate_field_and_option_target_field()?;
    Ok(())
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let target_field = args.take_string("target_field")?;
    let start = args.take_int_float_field("start")?;
    let end = args.take_int_float_field("end")?;
    Ok(Box::new(Random {
        target_field,
        start,
        end,
    }))
}

impl Computer for Random {
    fn compute(&mut self, message: &mut message::Message) {
        let mut rng = rand::thread_rng();
        let result = match (&self.start, &self.end) {
            (IntFloatFieldArg::ConstInt(start), IntFloatFieldArg::ConstInt(end)) => {
                let result = rng.gen_range(*start..=*end);
                MessageValue::Int64(result)
            }
            (IntFloatFieldArg::ConstInt(start), IntFloatFieldArg::ConstFloat(end)) => {
                let result = rng.gen_range(*start as f64..=*end);
                MessageValue::Float64(result)
            }
            (IntFloatFieldArg::ConstInt(start), IntFloatFieldArg::Field(end)) => {
                match message.get(end) {
                    Some(MessageValue::Int64(end)) => {
                        let result = rng.gen_range(*start..=*end);
                        MessageValue::Int64(result)
                    }
                    Some(MessageValue::Float64(end)) => {
                        let result = rng.gen_range(*start as f64..=*end);
                        MessageValue::Float64(result)
                    }
                    _ => MessageValue::Null,
                }
            }
            (IntFloatFieldArg::ConstFloat(start), IntFloatFieldArg::ConstInt(end)) => {
                let result = rng.gen_range(*start..=*end as f64);
                MessageValue::Float64(result)
            }
            (IntFloatFieldArg::ConstFloat(start), IntFloatFieldArg::ConstFloat(end)) => {
                let result = rng.gen_range(*start..=*end);
                MessageValue::Float64(result)
            }
            (IntFloatFieldArg::ConstFloat(start), IntFloatFieldArg::Field(end)) => {
                match message.get(end) {
                    Some(MessageValue::Int64(end)) => {
                        let result = rng.gen_range(*start..=*end as f64);
                        MessageValue::Float64(result)
                    }
                    Some(MessageValue::Float64(end)) => {
                        let result = rng.gen_range(*start..=*end);
                        MessageValue::Float64(result)
                    }
                    _ => MessageValue::Null,
                }
            }
            (IntFloatFieldArg::Field(start), IntFloatFieldArg::ConstInt(end)) => {
                match message.get(start) {
                    Some(MessageValue::Int64(start)) => {
                        let result = rng.gen_range(*start..=*end);
                        MessageValue::Int64(result)
                    }
                    Some(MessageValue::Float64(start)) => {
                        let result = rng.gen_range(*start..=*end as f64);
                        MessageValue::Float64(result)
                    }
                    _ => MessageValue::Null,
                }
            }
            (IntFloatFieldArg::Field(start), IntFloatFieldArg::ConstFloat(end)) => {
                match message.get(start) {
                    Some(MessageValue::Int64(start)) => {
                        let result = rng.gen_range(*start as f64..=*end);
                        MessageValue::Float64(result)
                    }
                    Some(MessageValue::Float64(start)) => {
                        let result = rng.gen_range(*start..=*end);
                        MessageValue::Float64(result)
                    }
                    _ => MessageValue::Null,
                }
            }
            (IntFloatFieldArg::Field(start), IntFloatFieldArg::Field(end)) => {
                match (message.get(start), message.get(end)) {
                    (Some(MessageValue::Int64(start)), Some(MessageValue::Int64(end))) => {
                        let result = rng.gen_range(*start..=*end);
                        MessageValue::Int64(result)
                    }
                    (Some(MessageValue::Int64(start)), Some(MessageValue::Float64(end))) => {
                        let result = rng.gen_range(*start as f64..=*end);
                        MessageValue::Float64(result)
                    }
                    (Some(MessageValue::Float64(start)), Some(MessageValue::Int64(end))) => {
                        let result = rng.gen_range(*start..=*end as f64);
                        MessageValue::Float64(result)
                    }
                    (Some(MessageValue::Float64(start)), Some(MessageValue::Float64(end))) => {
                        let result = rng.gen_range(*start..=*end);
                        MessageValue::Float64(result)
                    }
                    _ => MessageValue::Null,
                }
            }
        };

        message.add(self.target_field.clone(), result);
    }
}
