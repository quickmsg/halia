use anyhow::{bail, Result};
use common::get_dynamic_value_from_json;
use types::rules::functions::computer::ItemConf;

use crate::StringArg;

pub mod concat;
pub mod ends_with;
pub mod includes;
pub mod index_of;
pub mod length;
pub mod lower;
// pub mod lpad;
pub mod ltrim;
pub mod numbytes;
// pub mod regex_match;
pub mod regex_replace;
pub mod regex_substr;
pub mod reverse;
pub mod rtrim;
// pub mod slice;
// pub mod split;
// pub mod starts_with;
pub mod trim;
pub mod upper;

fn get_string_arg(conf: &ItemConf, key: &str) -> Result<StringArg> {
    let arg = conf
        .args
        .as_ref()
        .and_then(|conf_args| conf_args.get(key))
        .ok_or_else(|| anyhow::anyhow!("not found"))?;
    match get_dynamic_value_from_json(arg) {
        common::DynamicValue::Const(serde_json::Value::String(s)) => Ok(StringArg::Const(s)),
        common::DynamicValue::Const(_) => bail!("只支持字符串常量"),
        common::DynamicValue::Field(s) => Ok(StringArg::Field(s)),
    }
}

fn get_array_string_arg(conf: &ItemConf, key: &str) -> Result<Vec<StringArg>> {
    conf.args
        .as_ref()
        .and_then(|conf_args| conf_args.get(key))
        .ok_or_else(|| anyhow::anyhow!("concat function requires values arguments"))?
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("values arguments must be an array"))?
        .iter()
        .map(|right_arg| match get_dynamic_value_from_json(right_arg) {
            common::DynamicValue::Const(serde_json::Value::String(s)) => Ok(StringArg::Const(s)),
            common::DynamicValue::Const(_) => bail!("只支持字符串常量"),
            common::DynamicValue::Field(f) => Ok(StringArg::Field(f)),
        })
        .collect::<Result<Vec<_>>>()
}
