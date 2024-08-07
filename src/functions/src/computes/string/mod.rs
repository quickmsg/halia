mod length;
mod lower;
mod reverse;
mod upper;

use anyhow::{bail, Result};
use types::rules::functions::ComputerConfItem;

use super::Computer;

pub fn new(conf: ComputerConfItem) -> Result<Box<dyn Computer>> {
    match conf.name.as_str() {
        "length" => length::new(conf),
        "lower" => lower::new(conf),
        "upper" => upper::new(conf),
        "reverse" => reverse::new(conf),
        _ => bail!("不支持该函数"),
    }
}
