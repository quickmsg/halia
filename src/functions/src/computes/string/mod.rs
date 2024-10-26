mod length;
mod lower;
mod reverse;
mod upper;
// mod hex;
mod ltrim;
mod rtrim;
mod split;
mod trim;

use anyhow::{bail, Result};

use types::rules::functions::computer::ItemConf;

use super::Computer;

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    match conf.name.as_str() {
        "length" => length::new(conf),
        "lower" => lower::new(conf),
        "upper" => upper::new(conf),
        "reverse" => reverse::new(conf),
        _ => bail!("不支持该函数"),
    }
}
