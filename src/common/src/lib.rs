#![feature(concat_idents)]
pub mod error;
pub mod json;
pub mod storage;
pub mod ref_info;
pub mod sys;
pub mod sink_message_retain;
pub mod config;

pub fn check_page_size(i: usize, page: usize, size: usize) -> bool {
    i >= (page - 1) * size && i < page * size
}

pub enum DynamicValue {
    Const(serde_json::Value),
    Field(String),
}

pub fn get_dynamic_value_from_json(value: serde_json::Value) -> DynamicValue {
    match &value {
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::Array(_)
        | serde_json::Value::Object(_) => DynamicValue::Const(value),
        serde_json::Value::String(s) => {
            if s.starts_with("${") && s.ends_with("}") {
                DynamicValue::Field(s[2..s.len() - 1].to_string())
            } else {
                DynamicValue::Const(value)
            }
        }
    }
}
