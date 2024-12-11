use std::collections::HashMap;

use anyhow::{bail, Result};

use common::get_dynamic_value_from_json;

pub(crate) const FIELD_KEY: &str = "field";
pub(crate) const TARGET_FIELD_KEY: &str = "target_field";

pub(crate) struct Args(HashMap<String, serde_json::Value>);

impl From<HashMap<String, serde_json::Value>> for Args {
    fn from(args: HashMap<String, serde_json::Value>) -> Self {
        Args(args)
    }
}

impl Args {
    pub fn new(args: HashMap<String, serde_json::Value>) -> Self {
        Args(args)
    }

    pub fn take_field_and_option_target_field(&mut self) -> Result<(String, Option<String>)> {
        let field = self.take_string(FIELD_KEY)?;
        let target_field = self.take_option_string(TARGET_FIELD_KEY)?;
        Ok((field, target_field))
    }

    pub fn take_field_and_target_field(&mut self) -> Result<(String, String)> {
        let field = self.take_string(FIELD_KEY)?;
        let target_field = self.take_string(TARGET_FIELD_KEY)?;
        Ok((field, target_field))
    }

    pub fn validate_field_and_option_target_field(&mut self) -> Result<()> {
        let _ = self.take_string(FIELD_KEY)?;
        let _ = self.take_option_string(TARGET_FIELD_KEY)?;
        Ok(())
    }

    pub fn take_string_field(&mut self, key: &str) -> Result<StringFieldArg> {
        let arg = self
            .0
            .remove(key)
            .ok_or_else(|| anyhow::anyhow!("not found"))?;
        match get_dynamic_value_from_json(&arg) {
            common::DynamicValue::Const(serde_json::Value::String(s)) => {
                Ok(StringFieldArg::Const(s))
            }
            common::DynamicValue::Const(_) => bail!("只支持字符串常量"),
            common::DynamicValue::Field(s) => Ok(StringFieldArg::Field(s)),
        }
    }

    pub fn take_option_string_field(&mut self, key: &str) -> Result<Option<StringFieldArg>> {
        match self.0.remove(key) {
            Some(arg) => match get_dynamic_value_from_json(&arg) {
                common::DynamicValue::Const(serde_json::Value::String(s)) => {
                    Ok(Some(StringFieldArg::Const(s)))
                }
                common::DynamicValue::Const(_) => bail!("只支持字符串常量"),
                common::DynamicValue::Field(s) => Ok(Some(StringFieldArg::Field(s))),
            },
            None => Ok(None),
        }
    }

    pub fn take_string(&mut self, key: &str) -> Result<String> {
        let arg = self
            .0
            .remove(key)
            .ok_or_else(|| anyhow::Error::msg(format!("Key '{}' not found", key)))?;
        match arg {
            serde_json::Value::String(s) => Ok(s),
            _ => bail!("{} must be a string", key),
        }
    }

    pub fn take_option_string(&mut self, key: &str) -> Result<Option<String>> {
        self.0.remove(key).map_or(Ok(None), |arg| {
            if let serde_json::Value::String(s) = arg {
                Ok(Some(s))
            } else {
                bail!("{} must be a string", key)
            }
        })
    }

    pub fn take_array_string_field(&mut self, key: &str) -> Result<Vec<StringFieldArg>> {
        self.0
            .remove(key)
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

    pub fn take_number_field(&mut self, key: &str) -> Result<IntFloatFieldArg> {
        let arg = self
            .0
            .remove(key)
            .ok_or_else(|| anyhow::anyhow!("not found"))?;
        match get_dynamic_value_from_json(&arg) {
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

    pub fn take_int_float_field(&mut self, key: &str) -> Result<IntFloatFieldArg> {
        let arg = self
            .0
            .remove(key)
            .ok_or_else(|| anyhow::anyhow!("not found"))?;
        match get_dynamic_value_from_json(&arg) {
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
            common::DynamicValue::Field(f) => Ok(IntFloatFieldArg::Field(f)),
        }
    }

    pub fn take_array_int_float_field(&mut self, key: &str) -> Result<Vec<IntFloatFieldArg>> {
        self.0
            .remove(key)
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

    pub fn take_usize(&mut self, key: &str) -> Result<usize> {
        let arg = self
            .0
            .remove(key)
            .ok_or_else(|| anyhow::Error::msg(format!("Key '{}' not found", key)))?;
        match arg {
            serde_json::Value::Number(n) => Ok(n
                .as_u64()
                .ok_or_else(|| anyhow::Error::msg(format!("{} must be a u64", key)))?
                as usize),
            _ => bail!("{} must be a u64", key),
        }
    }

    pub fn take_bool_string_int_float_field(
        &mut self,
        key: &str,
    ) -> Result<BoolStringIntFloatFieldArg> {
        let arg = self
            .0
            .remove(key)
            .ok_or_else(|| anyhow::anyhow!("not found"))?;
        match arg {
            // serde_json::Value::Null => Ok(BoolStringIntFloatFieldArg::C)
            serde_json::Value::Bool(b) => Ok(BoolStringIntFloatFieldArg::ConstBool(b)),
            serde_json::Value::Number(number) => {
                if let Some(f) = number.as_f64() {
                    Ok(BoolStringIntFloatFieldArg::ConstFloat(f))
                } else if let Some(i) = number.as_i64() {
                    Ok(BoolStringIntFloatFieldArg::ConstInt(i))
                } else {
                    bail!("只支持数字常量")
                }
            }
            serde_json::Value::String(s) => Ok(BoolStringIntFloatFieldArg::ConstString(s)),
            _ => bail!("not support now"),
            // serde_json::Value::Array(vec) => todo!(),
            // serde_json::Value::Object(map) => todo!(),
        }
    }

    pub fn take_array_bool_string_int_float_field(
        &mut self,
        key: &str,
    ) -> Result<Vec<BoolStringIntFloatFieldArg>> {
        let arg = self
            .0
            .remove(key)
            .ok_or_else(|| anyhow::anyhow!("not found"))?;
        match arg {
            serde_json::Value::Array(vec) => {
                let mut values = Vec::with_capacity(vec.len());
                for v in vec.into_iter() {
                    let v = match v {
                        serde_json::Value::Null => todo!(),
                        serde_json::Value::Bool(b) => BoolStringIntFloatFieldArg::ConstBool(b),
                        serde_json::Value::Number(number) => {
                            if let Some(f) = number.as_f64() {
                                BoolStringIntFloatFieldArg::ConstFloat(f)
                            } else if let Some(i) = number.as_i64() {
                                BoolStringIntFloatFieldArg::ConstInt(i)
                            } else {
                                bail!("只支持数字常量")
                            }
                        }
                        serde_json::Value::String(s) => BoolStringIntFloatFieldArg::ConstString(s),
                        serde_json::Value::Array(_) => todo!(),
                        serde_json::Value::Object(_) => todo!(),
                    };
                    values.push(v);
                }

                Ok(values)
            }
            _ => bail!("{} must be an array", key),
        }
    }
}

pub(crate) enum StringFieldArg {
    Const(String),
    Field(String),
}

pub(crate) enum IntFloatFieldArg {
    ConstInt(i64),
    ConstFloat(f64),
    Field(String),
}

pub(crate) enum BoolStringIntFloatFieldArg {
    ConstBool(bool),
    ConstString(String),
    ConstInt(i64),
    ConstFloat(f64),
    Field(String),
}
