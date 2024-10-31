mod length;
mod lower;
mod reverse;
mod upper;
// mod hex;
mod ends_with;
mod indexof;
mod lpad;
mod ltrim;
mod numbytes;
mod regex_match;
mod regex_replace;
mod regex_substr;
mod rtrim;
mod split;
mod trim;
mod concat;
mod starts_with;

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
        types::rules::functions::computer::StringType::Split => split::new(conf),
        types::rules::functions::computer::StringType::Trim => trim::new(conf),
        types::rules::functions::computer::StringType::Endswith => ends_with::new(conf),
        types::rules::functions::computer::StringType::Indexof => indexof::new(conf),
        types::rules::functions::computer::StringType::Lpad => lpad::new(conf),
        types::rules::functions::computer::StringType::Numbytes => numbytes::new(conf),
        types::rules::functions::computer::StringType::RegexMatch => regex_match::new(conf),
        types::rules::functions::computer::StringType::Concat => concat::new(conf),
        types::rules::functions::computer::StringType::StartsWith => starts_with::new(conf),
    }
}
