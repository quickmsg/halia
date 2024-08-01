use std::f64::consts::PI;

use anyhow::Result;
use message::Message;

use crate::computes::Computer;

pub struct Atan {
    field: String,
    target_field: String,
}

impl Atan {
    pub fn new(field: String, target_field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Atan {
            field,
            target_field,
        }))
    }
}

impl Computer for Atan {
    fn compute(&self, message: &mut Message) {
        match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::Int64(mv) => {
                    if (*mv as f64) > -PI / 2.0 || (*mv as f64) > PI / 2.0 {
                        return;
                    }
                    message.add(
                        self.target_field.clone(),
                        message::MessageValue::Float64((*mv as f64).atan()),
                    )
                }
                message::MessageValue::Uint64(mv) => {
                    if (*mv as f64) > PI / 2.0 {
                        return;
                    }
                    message.add(
                        self.target_field.clone(),
                        message::MessageValue::Float64((*mv as f64).atan()),
                    )
                }
                message::MessageValue::Float64(mv) => {
                    if *mv < -PI / 2.0 || *mv > PI / 2.0 {
                        return;
                    }
                    message.add(
                        self.target_field.clone(),
                        message::MessageValue::Float64(mv.atan()),
                    )
                }
                _ => {}
            },
            None => {}
        }
    }
}
