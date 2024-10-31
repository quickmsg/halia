use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::computer::NumberItemConf;

use crate::add_or_set_message_value;

use super::Computer;

// 反余弦函数
struct Acos {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: NumberItemConf) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Acos {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for Acos {
    fn compute(&self, message: &mut Message) {
        let result = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => {
                    if *mv < -1 || *mv > 1 {
                        MessageValue::Null
                    } else {
                        MessageValue::Float64((*mv as f64).acos())
                    }
                }
                MessageValue::Float64(mv) => {
                    if *mv < -1.0 || *mv > 1.0 {
                        MessageValue::Null
                    } else {
                        MessageValue::Float64(mv.acos())
                    }
                }
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        add_or_set_message_value!(self, message, result);
    }
}
