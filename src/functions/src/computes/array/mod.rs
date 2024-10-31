use types::rules::functions::computer::ArrayItemConf;
use anyhow::Result;

use super::Computer;

mod append;
mod distinct;
mod cardinality;

pub fn new(conf: ArrayItemConf) -> Result<Box<dyn Computer>> {
    match conf.typ {
        types::rules::functions::computer::ArrayType::Cardinality => cardinality::new(conf),
    }
}