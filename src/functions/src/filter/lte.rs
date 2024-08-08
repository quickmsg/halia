use anyhow::{bail, Result};
use message::{Message, MessageValue};
use types::TargetValue;

use super::Filter;

struct LteConst {
    field: String,
    const_value: MessageValue,
}

struct LteDynamic {
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

            Ok(Box::new(LteConst { field, const_value }))
        }
        types::TargetValueType::Variable => match value.value {
            serde_json::Value::String(s) => Ok(Box::new(LteDynamic {
                field,
                target_field: s,
            })),
            _ => bail!("字段类型必须为字符串"),
        },
    }
}

impl Filter for LteConst {
    fn filter(&self, msg: &Message) -> bool {
        match msg.get(&self.field) {
            Some(mv) => lte(mv, &self.const_value),
            None => false,
        }
    }
}

impl Filter for LteDynamic {
    fn filter(&self, msg: &Message) -> bool {
        match (msg.get(&self.field), msg.get(&self.target_field)) {
            (Some(mv), Some(tv)) => lte(mv, tv),
            _ => false,
        }
    }
}

fn lte(mv: &MessageValue, tv: &MessageValue) -> bool {
    match (mv, tv) {
        (MessageValue::Int64(mv), MessageValue::Int64(tv)) => mv <= tv,
        (MessageValue::Float64(mv), MessageValue::Float64(tv)) => {
            mv - tv < -1e-10 || (mv - tv).abs() == 1e-10
        }
        _ => false,
    }
}
