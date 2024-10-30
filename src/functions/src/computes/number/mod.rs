mod abs;
mod acos;
mod acosh;
mod asin;
mod asinh;
mod atan;
// mod atan2;
mod atanh;
mod bitand;
mod bitnot;
mod bitor;
mod bitxor;
mod cbrt;
mod ceil;
mod cos;
mod cosh;
mod degrees;
mod exp;
mod exp2;
mod floor;
mod ln;
// mod log;
// mod power;
// mod add;
mod sin;

use anyhow::Result;
use types::rules::functions::computer::NumberItemConf;

use super::Computer;

pub fn new(conf: NumberItemConf) -> Result<Box<dyn Computer>> {
    match conf.typ {
        types::rules::functions::computer::NumberType::Abs => abs::new(conf),
        types::rules::functions::computer::NumberType::Acos => acos::new(conf),
        types::rules::functions::computer::NumberType::Acosh => acosh::new(conf),
        types::rules::functions::computer::NumberType::Add => todo!(),
        types::rules::functions::computer::NumberType::Asin => asin::new(conf),
        types::rules::functions::computer::NumberType::Asinh => asinh::new(conf),
        types::rules::functions::computer::NumberType::Atan => atan::new(conf),
        types::rules::functions::computer::NumberType::Atan2 => todo!(),
        types::rules::functions::computer::NumberType::Atanh => atanh::new(conf),
        types::rules::functions::computer::NumberType::Bitand => bitand::new(conf),
        types::rules::functions::computer::NumberType::Bitnot => bitnot::new(conf),
        types::rules::functions::computer::NumberType::Bitor => bitor::new(conf),
        types::rules::functions::computer::NumberType::Bitxor => bitxor::new(conf),
        types::rules::functions::computer::NumberType::Cbrt => cbrt::new(conf),
        types::rules::functions::computer::NumberType::Ceil => ceil::new(conf),
        types::rules::functions::computer::NumberType::Cos => cos::new(conf),
        types::rules::functions::computer::NumberType::Cosh => cosh::new(conf),
        types::rules::functions::computer::NumberType::Degrees => degrees::new(conf),
        types::rules::functions::computer::NumberType::Exp => exp::new(conf),
        types::rules::functions::computer::NumberType::Exp2 => exp2::new(conf),
        types::rules::functions::computer::NumberType::Floor => floor::new(conf),
        types::rules::functions::computer::NumberType::Ln => ln::new(conf),
        types::rules::functions::computer::NumberType::Log => todo!(),
        types::rules::functions::computer::NumberType::Power => todo!(),
        types::rules::functions::computer::NumberType::Sin => sin::new(conf),
    }
}
