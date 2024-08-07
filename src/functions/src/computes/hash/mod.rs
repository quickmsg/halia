use anyhow::{bail, Result};
use types::rules::functions::ComputerConfItem;

mod md5;
mod sha1;

use super::Computer;

pub fn new(conf: ComputerConfItem) -> Result<Box<dyn Computer>> {
    match conf.name.as_str() {
        "md5" => md5::new(conf),
        "sha1" => sha1::new(conf),
        _ => bail!("不支持该函数"),
    }
}
