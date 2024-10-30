use anyhow::Result;
use types::rules::functions::computer::HashItemConf;

mod md5;
mod sha1;
mod sha256;

use super::Computer;

pub fn new(conf: HashItemConf) -> Result<Box<dyn Computer>> {
    match conf.typ {
        types::rules::functions::computer::HashType::Md5 => md5::new(conf),
        types::rules::functions::computer::HashType::Sha1 => sha1::new(conf),
        types::rules::functions::computer::HashType::Sha256 => todo!(),
    }
}
