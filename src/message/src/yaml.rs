use std::collections::HashMap;

use yaml_rust2::yaml::Hash;

use crate::{Message, MessageValue};

impl From<yaml_rust2::Yaml> for Message {
    fn from(value: yaml_rust2::Yaml) -> Self {
        Self {
            metadatas: HashMap::new(),
            value: MessageValue::from(value),
        }
    }
}

impl From<yaml_rust2::Yaml> for MessageValue {
    fn from(value: yaml_rust2::Yaml) -> Self {
        match value {
            yaml_rust2::Yaml::Real(f) => {
                // TODO 避免unwrap
                Self::Float64(f.parse::<f64>().unwrap())
            }
            yaml_rust2::Yaml::Integer(i) => Self::Int64(i),
            yaml_rust2::Yaml::String(s) => Self::String(s),
            yaml_rust2::Yaml::Boolean(b) => Self::Boolean(b),
            yaml_rust2::Yaml::Array(vec) => Self::Array(vec.into_iter().map(Self::from).collect()),
            yaml_rust2::Yaml::Hash(linked_hash_map) => Self::Object(
                linked_hash_map
                    .into_iter()
                    .filter_map(|(key, value)| key.into_string().map(|k| (k, Self::from(value))))
                    .collect(),
            ),
            yaml_rust2::Yaml::Alias(_) => todo!(),
            yaml_rust2::Yaml::Null => Self::Null,
            yaml_rust2::Yaml::BadValue => todo!(),
        }
    }
}

impl Into<yaml_rust2::Yaml> for MessageValue {
    fn into(self) -> yaml_rust2::Yaml {
        match self {
            MessageValue::Null => yaml_rust2::Yaml::Null,
            MessageValue::Boolean(b) => yaml_rust2::Yaml::Boolean(b),
            MessageValue::Int64(i) => yaml_rust2::Yaml::Integer(i),
            MessageValue::Float64(f) => yaml_rust2::Yaml::Real(f.to_string()),
            MessageValue::String(s) => yaml_rust2::Yaml::String(s),
            MessageValue::Bytes(vec) => todo!(),
            MessageValue::Array(vec) => {
                yaml_rust2::Yaml::Array(vec.into_iter().map(Into::into).collect())
            }
            MessageValue::Object(hash_map) => {
                let mut linked_hash_map = Hash::new();
                for (k, v) in hash_map {
                    linked_hash_map.insert(yaml_rust2::Yaml::String(k), v.into());
                }
                yaml_rust2::Yaml::Hash(linked_hash_map)
            }
        }
    }
}
