mod length;
mod lower;
mod reverse;
mod upper;
// mod hex;
mod endswith;
mod indexof;
mod lpad;
mod ltrim;
mod rtrim;
mod split;
mod trim;

use anyhow::Result;

use types::rules::functions::computer::StringItemConf;

use super::Computer;

pub fn new(conf: StringItemConf) -> Result<Box<dyn Computer>> {
    match conf.typ {
        types::rules::functions::computer::StringType::Base64 => todo!(),
        types::rules::functions::computer::StringType::Hex => todo!(),
        types::rules::functions::computer::StringType::Length => length::new(conf),
        types::rules::functions::computer::StringType::Lower => lower::new(conf),
        types::rules::functions::computer::StringType::Upper => upper::new(conf),
        types::rules::functions::computer::StringType::Ltrim => ltrim::new(conf),
        types::rules::functions::computer::StringType::Reverse => reverse::new(conf),
        types::rules::functions::computer::StringType::Rtrim => rtrim::new(conf),
        types::rules::functions::computer::StringType::Split => todo!(),
        types::rules::functions::computer::StringType::Trim => trim::new(conf),
        types::rules::functions::computer::StringType::Endswith => endswith::new(conf),
        types::rules::functions::computer::StringType::Indexof => indexof::new(conf),
        types::rules::functions::computer::StringType::Lpad => lpad::new(conf),
    }
}
