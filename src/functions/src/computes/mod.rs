use anyhow::Result;
use async_trait::async_trait;
use message::Message;
use types::rules::functions::computer::Conf;

use crate::Function;

mod array;
mod compress;
mod hash;
mod number;
mod string;

pub trait Computer: Sync + Send {
    fn compute(&self, message: &mut Message);
}

pub struct Node {
    computers: Vec<Box<dyn Computer>>,
}

pub fn new(conf: Conf) -> Result<Box<dyn Function>> {
    let mut computers: Vec<Box<dyn Computer>> = Vec::with_capacity(conf.items.len());
    for item_conf in conf.items {
        let computer = match item_conf.typ {
            // number
            types::rules::functions::computer::Type::NumberAbs => number::abs::new(item_conf)?,
            types::rules::functions::computer::Type::NumberAcos => number::acos::new(item_conf)?,
            types::rules::functions::computer::Type::NumberAcosh => number::acosh::new(item_conf)?,
            types::rules::functions::computer::Type::NumberAdd => number::add::new(item_conf)?,
            types::rules::functions::computer::Type::NumberAsin => number::asin::new(item_conf)?,
            types::rules::functions::computer::Type::NumberAsinh => number::asinh::new(item_conf)?,
            types::rules::functions::computer::Type::NumberAtan => number::atan::new(item_conf)?,
            types::rules::functions::computer::Type::NumberAtan2 => {
                //  number::atan2::new(item_conf)?,
                todo!()
            }
            types::rules::functions::computer::Type::NumberAtanh => number::atanh::new(item_conf)?,
            types::rules::functions::computer::Type::NumberBitand => {
                number::bitand::new(item_conf)?
            }
            types::rules::functions::computer::Type::NumberBitnot => {
                number::bitnot::new(item_conf)?
            }
            types::rules::functions::computer::Type::NumberBitor => number::bitor::new(item_conf)?,
            types::rules::functions::computer::Type::NumberBitxor => {
                number::bitxor::new(item_conf)?
            }
            types::rules::functions::computer::Type::NumberCbrt => number::cbrt::new(item_conf)?,
            types::rules::functions::computer::Type::NumberCeil => number::ceil::new(item_conf)?,
            types::rules::functions::computer::Type::NumberCos => number::cos::new(item_conf)?,
            types::rules::functions::computer::Type::NumberCosh => number::cosh::new(item_conf)?,
            types::rules::functions::computer::Type::NumberDegrees => {
                number::degrees::new(item_conf)?
            }
            types::rules::functions::computer::Type::NumberExp => number::exp::new(item_conf)?,
            types::rules::functions::computer::Type::NumberExp2 => number::exp2::new(item_conf)?,
            types::rules::functions::computer::Type::NumberFloor => number::floor::new(item_conf)?,
            types::rules::functions::computer::Type::NumberLn => number::ln::new(item_conf)?,
            types::rules::functions::computer::Type::NumberLog => todo!(),
            types::rules::functions::computer::Type::NumberPower => todo!(),
            types::rules::functions::computer::Type::NumberSin => number::sin::new(item_conf)?,
            types::rules::functions::computer::Type::NumberSub => number::sub::new(item_conf)?,
            types::rules::functions::computer::Type::NumberMulti => number::multi::new(item_conf)?,
            types::rules::functions::computer::Type::NumberDivision => {
                number::division::new(item_conf)?
            }
            types::rules::functions::computer::Type::NumberModulo => {
                number::modulo::new(item_conf)?
            }

            // string
            types::rules::functions::computer::Type::StringBase64 => {
                // string::base64::new(item_conf)?
                todo!()
            }
            types::rules::functions::computer::Type::StringHex => {
                // string::hex::new(item_conf)?;
                todo!()
            }
            types::rules::functions::computer::Type::StringLength => {
                string::length::new(item_conf)?
            }
            types::rules::functions::computer::Type::StringLower => string::lower::new(item_conf)?,
            types::rules::functions::computer::Type::StringUpper => string::upper::new(item_conf)?,
            types::rules::functions::computer::Type::StringLtrim => string::ltrim::new(item_conf)?,
            // types::rules::functions::computer::Type::StringLpad => string::lpad::new(item_conf)?,
            types::rules::functions::computer::Type::StringLpad => todo!(),
            types::rules::functions::computer::Type::StringReverse => {
                string::reverse::new(item_conf)?
            }
            types::rules::functions::computer::Type::StringRtrim => string::rtrim::new(item_conf)?,
            // types::rules::functions::computer::Type::StringSplit => string::split::new(item_conf)?,
            types::rules::functions::computer::Type::StringSplit => todo!(),
            types::rules::functions::computer::Type::StringTrim => string::trim::new(item_conf)?,
            types::rules::functions::computer::Type::StringEndsWith => {
                string::ends_with::new(item_conf)?
            }
            types::rules::functions::computer::Type::StringStartsWith => {
                // string::starts_with::new(item_conf)?
                todo!()
            }
            types::rules::functions::computer::Type::StringIndexOf => {
                string::index_of::new(item_conf)?
            }
            types::rules::functions::computer::Type::StringNumbytes => {
                string::numbytes::new(item_conf)?
            }
            types::rules::functions::computer::Type::StringRegexMatch => {
                // string::regex_match::new(item_conf)?
                todo!()
            }
            types::rules::functions::computer::Type::StringConcat => {
                string::concat::new(item_conf)?
            }
            // types::rules::functions::computer::Type::StringSlice => string::slice::new(item_conf)?,
            types::rules::functions::computer::Type::StringSlice => todo!(),
            // hash
            types::rules::functions::computer::Type::HashMd5 => hash::md5::new(item_conf)?,
            types::rules::functions::computer::Type::HashSha1 => hash::sha1::new(item_conf)?,
            types::rules::functions::computer::Type::HashSha256 => todo!(),
            types::rules::functions::computer::Type::Date => todo!(),
            types::rules::functions::computer::Type::ArrayCardinality => todo!(),
            // compress
            types::rules::functions::computer::Type::CompressBrotli => {
                compress::brotli::new_encoder(item_conf)
            }
            types::rules::functions::computer::Type::DecompressBrotli => {
                compress::brotli::new_decoder(item_conf)
            }
            types::rules::functions::computer::Type::CompressDeflate => {
                compress::deflate::new_encoder(item_conf)
            }
            types::rules::functions::computer::Type::DecompressDeflate => {
                compress::deflate::new_decoder(item_conf)
            }
            types::rules::functions::computer::Type::CompressGzip => {
                compress::gzip::new_encoder(item_conf)
            }
            types::rules::functions::computer::Type::DecompressGzip => {
                compress::gzip::new_decoder(item_conf)
            }
            types::rules::functions::computer::Type::CompressLz4 => {
                compress::lz4::new_encoder(item_conf)
            }
            types::rules::functions::computer::Type::DecompressLz4 => {
                compress::lz4::new_decoder(item_conf)
            }
            types::rules::functions::computer::Type::CompressSnappy => {
                compress::snappy::new_encoder(item_conf)
            }
            types::rules::functions::computer::Type::DecompressSnappy => {
                compress::snappy::new_decoder(item_conf)
            }
            types::rules::functions::computer::Type::CompressZlib => {
                compress::zlib::new_encoder(item_conf)
            }
            types::rules::functions::computer::Type::DecompressZlib => {
                compress::zlib::new_decoder(item_conf)
            }
        };
        computers.push(computer);
    }
    Ok(Box::new(Node { computers }))
}

#[async_trait]
impl Function for Node {
    async fn call(&self, message_batch: &mut message::MessageBatch) -> bool {
        let messages = message_batch.get_messages_mut();
        messages.iter_mut().for_each(|msg| {
            for computer in &self.computers {
                computer.compute(msg);
            }
        });

        true
    }
}
