use std::f64::consts::PI;

use anyhow::Result;
use message::{Message, MessageValue};

use crate::{computes::Computer, Args};

pub struct Atan {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = crate::get_field_and_option_target_field(&mut args)?;
    Ok(Box::new(Atan {
        field,
        target_field,
    }))
}

impl Computer for Atan {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => {
                    if (*mv as f64) > -PI / 2.0 || (*mv as f64) > PI / 2.0 {
                        MessageValue::Null
                    } else {
                        MessageValue::Float64((*mv as f64).atan())
                    }
                }
                MessageValue::Float64(mv) => {
                    if *mv < -PI / 2.0 || *mv > PI / 2.0 {
                        MessageValue::Null
                    } else {
                        MessageValue::Float64(mv.atan())
                    }
                }
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        match &self.target_field {
            Some(target_field) => message.add(target_field.clone(), value),
            None => message.set(&self.field, value),
        }
    }
}
