use std::collections::HashMap;

use anyhow::{bail, Result};
use async_trait::async_trait;
use common::get_dynamic_value_from_json;
use message::MessageBatch;
use types::rules::functions::ItemConf;

pub mod aggregation;
pub mod computes;
pub mod field;
pub mod filter;
pub mod merge;
pub mod metadata;
pub mod type_conversion;
pub mod type_judgment;
pub mod window;

const FIELD_KEY: &str = "field";
const TARGET_FIELD_KEY: &str = "target_field";

type Args = HashMap<String, serde_json::Value>;

#[macro_export]
macro_rules! add_or_set_message_value {
    ($self:expr, $message:expr, $value:expr) => {
        match &$self.target_field {
            Some(target_field) => $message.add(target_field.clone(), $value),
            None => $message.set(&$self.field, $value),
        }
    };
}

#[async_trait]
pub trait Function: Send + Sync {
    // 修改消息，根据返回值判断是否要继续流程，为false则消息丢弃
    async fn call(&mut self, message_batch: &mut MessageBatch) -> bool;
}

enum StringFieldArg {
    Const(String),
    Field(String),
}

fn get_string_field_arg(args: &mut Args, key: &str) -> Result<StringFieldArg> {
    let arg = args
        .remove(key)
        .ok_or_else(|| anyhow::anyhow!("not found"))?;
    match get_dynamic_value_from_json(&arg) {
        common::DynamicValue::Const(serde_json::Value::String(s)) => Ok(StringFieldArg::Const(s)),
        common::DynamicValue::Const(_) => bail!("只支持字符串常量"),
        common::DynamicValue::Field(s) => Ok(StringFieldArg::Field(s)),
    }
}

fn get_array_string_field_arg(args: &mut Args, key: &str) -> Result<Vec<StringFieldArg>> {
    args.remove(key)
        .ok_or_else(|| anyhow::anyhow!("concat function requires values arguments"))?
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("values arguments must be an array"))?
        .iter()
        .map(|right_arg| match get_dynamic_value_from_json(right_arg) {
            common::DynamicValue::Const(serde_json::Value::String(s)) => {
                Ok(StringFieldArg::Const(s))
            }
            common::DynamicValue::Const(_) => bail!("只支持字符串常量"),
            common::DynamicValue::Field(f) => Ok(StringFieldArg::Field(f)),
        })
        .collect::<Result<Vec<_>>>()
}

enum IntFloatFieldArg {
    ConstInt(i64),
    ConstFloat(f64),
    Field(String),
}

fn get_int_float_field_arg(conf: &ItemConf, key: &str) -> Result<IntFloatFieldArg> {
    let arg = conf
        .args
        .get(key)
        .ok_or_else(|| anyhow::anyhow!("not found"))?;
    match get_dynamic_value_from_json(arg) {
        common::DynamicValue::Const(serde_json::Value::Number(i)) => {
            if let Some(f) = i.as_f64() {
                Ok(IntFloatFieldArg::ConstFloat(f))
            } else if let Some(i) = i.as_i64() {
                Ok(IntFloatFieldArg::ConstInt(i))
            } else {
                bail!("只支持数字常量")
            }
        }
        common::DynamicValue::Const(_) => bail!("只支持数字常量"),
        common::DynamicValue::Field(s) => Ok(IntFloatFieldArg::Field(s)),
    }
}

fn get_array_int_float_field_arg(args: &mut Args, key: &str) -> Result<Vec<IntFloatFieldArg>> {
    args.remove(key)
        .ok_or_else(|| anyhow::anyhow!("concat function requires values arguments"))?
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("values arguments must be an array"))?
        .iter()
        .map(|right_arg| match get_dynamic_value_from_json(right_arg) {
            common::DynamicValue::Const(serde_json::Value::Number(n)) => {
                if let Some(f) = n.as_f64() {
                    Ok(IntFloatFieldArg::ConstFloat(f))
                } else if let Some(i) = n.as_i64() {
                    Ok(IntFloatFieldArg::ConstInt(i))
                } else {
                    bail!("只支持数字常量")
                }
            }
            common::DynamicValue::Const(_) => bail!("只支持字符串常量"),
            common::DynamicValue::Field(f) => Ok(IntFloatFieldArg::Field(f)),
        })
        .collect::<Result<Vec<_>>>()
}

fn get_string_arg(args: &mut Args, key: &str) -> Result<String> {
    let arg = args
        .remove(key)
        .ok_or_else(|| anyhow::Error::msg(format!("Key '{}' not found", key)))?;
    match arg {
        serde_json::Value::String(s) => Ok(s),
        _ => bail!("{} must be a string", key),
    }
}

fn get_option_string_arg(args: &mut Args, key: &str) -> Result<Option<String>> {
    args.get(key).map_or(Ok(None), |arg| {
        if let serde_json::Value::String(s) = arg {
            Ok(Some(s.clone()))
        } else {
            bail!("{} must be a string", key)
        }
    })
}

fn get_field_and_option_target_field(args: &mut Args) -> Result<(String, Option<String>)> {
    let field = get_string_arg(args, FIELD_KEY)?;
    let target_field = get_option_string_arg(args, TARGET_FIELD_KEY)?;
    Ok((field, target_field))
}
