mod abs;
mod acos;
mod acosh;
mod asin;
mod asinh;
mod atan;
// mod atan2;
mod atanh;
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

use anyhow::{bail, Result};
use types::rules::functions::computer::ItemConf;

use super::Computer;

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    match conf.name.as_str() {
        "abs" => abs::new(conf),
        "acos" => acos::new(conf),
        "acosh" => acosh::new(conf),
        "asin" => asin::new(conf),
        "asinh" => asinh::new(conf),
        "atan" => atan::new(conf),
        // "atan2" => atan2::new(conf),
        "atanh" => atanh::new(conf),
        "cbrt" => cbrt::new(conf),
        "ceil" => ceil::new(conf),
        "cos" => cos::new(conf),
        "cosh" => cosh::new(conf),
        "degrees" => degrees::new(conf),
        "exp" => exp::new(conf),
        "exp2" => exp2::new(conf),
        "floor" => floor::new(conf),
        "ln" => ln::new(conf),
        // "log" => log::new(conf),
        "sin" => sin::new(conf),
        _ => bail!("不支持该函数"),
    }
}
