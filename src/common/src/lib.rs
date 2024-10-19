use uuid::Uuid;

pub mod config;
pub mod constants;
pub mod error;
pub mod json;
pub mod ref_info;
pub mod sink_message_retain;
pub mod sys;

pub enum DynamicValue {
    Const(serde_json::Value),
    Field(String),
}

pub fn get_dynamic_value_from_json(value: &serde_json::Value) -> DynamicValue {
    match value {
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::Array(_)
        | serde_json::Value::Object(_) => DynamicValue::Const(value.clone()),
        serde_json::Value::String(s) => {
            if s.starts_with("${") && s.ends_with("}") {
                DynamicValue::Field(s[2..s.len() - 1].to_string())
            } else {
                DynamicValue::Const(value.clone())
            }
        }
    }
}

pub fn get_id() -> String {
    Uuid::new_v4().simple().to_string()
}

pub fn timestamp_millis() -> i64 {
    let now = std::time::SystemTime::now();
    now.duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

pub fn vec_to_string(v: Option<Vec<u8>>) -> Option<String> {
    match v {
        Some(v) => unsafe { Some(String::from_utf8_unchecked(v)) },
        None => None,
    }
}
