pub mod abs;
pub mod acos;
pub mod acosh;
pub mod asin;
pub mod asinh;
pub mod atan;
pub mod atan2;
pub mod atanh;
pub mod cbrt;
pub mod ceil;
pub mod cos;
pub mod cosh;
pub mod degrees;
pub mod exp;
// pub mod factorial;
pub mod floor;
// pub mod gcd;
pub mod ln;
pub mod log;
pub mod power;
pub mod sin;

use anyhow::{bail, Result};

use self::{
    abs::Abs, acos::Acos, acosh::Acosh, asin::Asin, asinh::Asinh, atan::Atan, atan2::Atan2,
    atanh::Atanh, cbrt::Cbrt, ceil::Ceil, cos::Cos, cosh::Cosh, floor::Floor, ln::Ln, sin::Sin,
};

use super::{Computer, Rule};

pub(crate) fn get_float_computer(rule: &Rule) -> Result<Box<dyn Computer>> {
    match rule.name.as_str() {
        "abs" => Abs::new(rule.field.clone()),
        "acos" => Acos::new(rule.field.clone()),
        "acosh" => Acosh::new(rule.field.clone()),
        "asin" => Asin::new(rule.field.clone()),
        "asinh" => Asinh::new(rule.field.clone()),
        "atan" => Atan::new(rule.field.clone()),
        // TODO other arguments
        // "atan2" => Atan2::new(rule.field.clone()),
        "atanh" => Atanh::new(rule.field.clone()),
        "cbrt" => Cbrt::new(rule.field.clone()),
        "ceil" => Ceil::new(rule.field.clone()),
        "cos" => Cos::new(rule.field.clone()),
        "cosh" => Cosh::new(rule.field.clone()),
        "floor" => Floor::new(rule.field.clone()),
        "ln" => Ln::new(rule.field.clone()),
        "sin" => Sin::new(rule.field.clone()),
        _ => bail!("not support"),
    }
}
