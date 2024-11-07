use anyhow::{bail, Result};
use common::get_dynamic_value_from_json;
use message::{Message, MessageValue};
use types::rules::functions::computer::ItemConf;

use crate::{add_or_set_message_value, computes::Computer};

struct Sub {
    field: String,
    target_field: Option<String>,
    args: Vec<Arg>,
}

enum Arg {
    ConstInt(i64),
    ConstFloat(f64),
    DynamicValueField(String),
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    let mut args = vec![];
    match conf.args {
        Some(conf_args) => {
            for conf_arg in conf_args {
                match get_dynamic_value_from_json(&conf_arg) {
                    common::DynamicValue::Const(value) => match value {
                        serde_json::Value::Number(number) => {
                            if let Some(i) = number.as_i64() {
                                args.push(Arg::ConstInt(i));
                            } else if let Some(f) = number.as_f64() {
                                args.push(Arg::ConstFloat(f));
                            } else {
                                bail!("add function needs a const number value");
                            }
                        }
                        _ => bail!("add function needs a const number value"),
                    },
                    common::DynamicValue::Field(field) => args.push(Arg::DynamicValueField(field)),
                }
            }
        }
        None => bail!("add function needs a const value"),
    }

    Ok(Box::new(Sub {
        field: conf.field,
        target_field: conf.target_field,
        args,
    }))
}

impl Computer for Sub {
    fn compute(&self, message: &mut Message) {
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
                Arg::ConstInt(i) => match result {
                    MessageValue::Int64(v) => result = MessageValue::Int64(v - i),
                    MessageValue::Float64(v) => result = MessageValue::Float64(v - *i as f64),
                    _ => return,
                },
                Arg::ConstFloat(f) => match result {
                    MessageValue::Int64(v) => result = MessageValue::Float64(v as f64 - f),
                    MessageValue::Float64(v) => result = MessageValue::Float64(v - f),
                    _ => return,
                },
                Arg::DynamicValueField(field) => match message.get(field) {
                    Some(mv) => match mv {
                        MessageValue::Int64(i) => match result {
                            MessageValue::Int64(v) => result = MessageValue::Int64(v - i),
                            MessageValue::Float64(v) => {
                                result = MessageValue::Float64(v - *i as f64)
                            }
                            _ => return,
                        },
                        MessageValue::Float64(f) => match result {
                            MessageValue::Int64(v) => result = MessageValue::Float64(v as f64 - f),
                            MessageValue::Float64(v) => result = MessageValue::Float64(v - f),
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
