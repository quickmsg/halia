use anyhow::{bail, Result};
use message::{Message, MessageValue};
use types::TargetValue;

use super::Filter;

struct NeqConst {
    field: String,
    const_value: MessageValue,
}

struct NeqDynamic {
    field: String,
    target_field: String,
}

pub fn new(field: String, value: TargetValue) -> Result<Box<dyn Filter>> {
    match value.typ {
        types::TargetValueType::Const => {
            let const_value = match value.value {
                serde_json::Value::Number(v) => MessageValue::from_json_number(v)?,
                _ => bail!("不支持该类型"),
            };

            Ok(Box::new(NeqConst { field, const_value }))
        }
        types::TargetValueType::Variable => match value.value {
            serde_json::Value::String(s) => Ok(Box::new(NeqDynamic {
                field,
                target_field: s,
            })),
            _ => bail!("字段名必须为字符串类型"),
        },
    }
}

impl Filter for NeqConst {
    fn filter(&self, msg: &Message) -> bool {
        match msg.get(&self.field) {
            Some(mv) => neq(mv, &self.const_value),
            None => false,
        }
    }
}

impl Filter for NeqDynamic {
    fn filter(&self, msg: &Message) -> bool {
        match (msg.get(&self.field), msg.get(&self.target_field)) {
            (Some(mv), Some(tv)) => neq(mv, tv),
            _ => false,
        }
    }
}

fn neq(mv: &MessageValue, tv: &MessageValue) -> bool {
    match (mv, tv) {
        (MessageValue::Boolean(mv), MessageValue::Boolean(tv)) => mv != tv,
        (MessageValue::Int64(mv), MessageValue::Int64(tv)) => mv != tv,
        (MessageValue::Float64(mv), MessageValue::Float64(tv)) => (mv - tv).abs() > 1e-10,
        (MessageValue::String(_), MessageValue::String(_)) => todo!(),
        (MessageValue::Bytes(mv), MessageValue::Bytes(tv)) => mv != tv,
        (MessageValue::Array(_), MessageValue::Array(_)) => todo!(),
        (MessageValue::Object(_), MessageValue::Object(_)) => todo!(),
        _ => false,
    }
}
