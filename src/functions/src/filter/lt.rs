use anyhow::{bail, Result};
use common::get_dynamic_value_from_json;
use message::{Message, MessageValue};

use super::Filter;

struct LtConst {
    field: String,
    const_value: MessageValue,
}

struct LtDynamic {
    field: String,
    target_field: String,
}

pub fn new(field: String, value: serde_json::Value) -> Result<Box<dyn Filter>> {
    match get_dynamic_value_from_json(&value) {
        common::DynamicValue::Const(value) => {
            let const_value = match value {
                serde_json::Value::Number(v) => MessageValue::from_json_number(v)?,
                _ => bail!("不支持该类型"),
            };

            Ok(Box::new(LtConst { field, const_value }))
        }
        common::DynamicValue::Field(s) => Ok(Box::new(LtDynamic {
            field,
            target_field: s,
        })),
    }
}

impl Filter for LtConst {
    fn filter(&self, msg: &Message) -> bool {
        match msg.get(&self.field) {
            Some(mv) => lt(mv, &self.const_value),
            None => false,
        }
    }
}

impl Filter for LtDynamic {
    fn filter(&self, msg: &Message) -> bool {
        match (msg.get(&self.field), msg.get(&self.target_field)) {
            (Some(mv), Some(tv)) => lt(mv, tv),
            _ => false,
        }
    }
}

fn lt(mv: &MessageValue, tv: &MessageValue) -> bool {
    match (mv, tv) {
        (MessageValue::Int64(mv), MessageValue::Int64(tv)) => mv < tv,
        (MessageValue::Float64(mv), MessageValue::Float64(tv)) => mv - tv < -1e-10,
        _ => false,
    }
}
