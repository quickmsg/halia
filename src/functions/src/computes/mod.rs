use anyhow::Result;
use async_trait::async_trait;
use message::Message;
use types::rules::functions::Conf;

use crate::Function;

mod array;
mod compress;
mod hash;
mod number;
mod string;

pub trait Computer: Sync + Send {
    fn compute(&mut self, message: &mut Message);
}

pub struct Node {
    computers: Vec<Box<dyn Computer>>,
}

pub fn new(conf: Conf) -> Result<Box<dyn Function>> {
    let mut computers: Vec<Box<dyn Computer>> = Vec::with_capacity(conf.items.len());
    for item_conf in conf.items {
        let computer = match item_conf.typ {
            // number
            types::rules::functions::Type::NumberAbs => number::abs::new(item_conf.args)?,
            types::rules::functions::Type::NumberAcos => number::acos::new(item_conf.args)?,
            types::rules::functions::Type::NumberAcosh => number::acosh::new(item_conf.args)?,
            types::rules::functions::Type::NumberAdd => number::add::new(item_conf.args)?,
            types::rules::functions::Type::NumberAsin => number::asin::new(item_conf.args)?,
            types::rules::functions::Type::NumberAsinh => number::asinh::new(item_conf.args)?,
            types::rules::functions::Type::NumberAtan => number::atan::new(item_conf.args)?,
            types::rules::functions::Type::NumberAtan2 => {
                //  number::atan2::new(item_conf)?,
                todo!()
            }
            types::rules::functions::Type::NumberAtanh => number::atanh::new(item_conf.args)?,
            types::rules::functions::Type::NumberBitand => {
                todo!()
                // number::bitand::new(item_conf)?
            }
            types::rules::functions::Type::NumberBitnot => {
                todo!()
                // number::bitnot::new(item_conf)?
            }
            // types::rules::functions::Type::NumberBitor => number::bitor::new(item_conf)?,
            types::rules::functions::Type::NumberBitor => todo!(),
            types::rules::functions::Type::NumberBitxor => {
                todo!()
                // number::bitxor::new(item_conf)?
            }

            // string
            types::rules::functions::Type::StringNew => string::new::new(item_conf.args)?,
            types::rules::functions::Type::NumberCbrt => number::cbrt::new(item_conf.args)?,
            types::rules::functions::Type::NumberCeil => number::ceil::new(item_conf.args)?,
            types::rules::functions::Type::NumberCos => number::cos::new(item_conf.args)?,
            types::rules::functions::Type::NumberCosh => number::cosh::new(item_conf.args)?,
            types::rules::functions::Type::NumberDegrees => number::degrees::new(item_conf.args)?,
            types::rules::functions::Type::NumberExp => number::exp::new(item_conf.args)?,
            types::rules::functions::Type::NumberExp2 => number::exp2::new(item_conf.args)?,
            types::rules::functions::Type::NumberFloor => number::floor::new(item_conf.args)?,
            types::rules::functions::Type::NumberLn => number::ln::new(item_conf.args)?,
            types::rules::functions::Type::NumberLog => todo!(),
            types::rules::functions::Type::NumberPower => todo!(),
            types::rules::functions::Type::NumberSin => number::sin::new(item_conf.args)?,
            types::rules::functions::Type::NumberSub => number::sub::new(item_conf.args)?,
            types::rules::functions::Type::NumberMulti => number::multi::new(item_conf.args)?,
            types::rules::functions::Type::NumberDivision => number::division::new(item_conf.args)?,
            types::rules::functions::Type::NumberModulo => number::modulo::new(item_conf.args)?,

            // string
            types::rules::functions::Type::StringBase64 => {
                // string::base64::new(item_conf)?
                todo!()
            }
            types::rules::functions::Type::StringHex => {
                // string::hex::new(item_conf)?;
                todo!()
            }
            types::rules::functions::Type::StringLength => string::length::new(item_conf.args)?,
            types::rules::functions::Type::StringLower => string::lower::new(item_conf.args)?,
            types::rules::functions::Type::StringUpper => string::upper::new(item_conf.args)?,
            types::rules::functions::Type::StringTrimStart => {
                string::trim_start::new(item_conf.args)?
            }
            types::rules::functions::Type::StringReverse => string::reverse::new(item_conf.args)?,
            types::rules::functions::Type::StringTrimEnd => string::trim_end::new(item_conf.args)?,
            types::rules::functions::Type::StringSplit => todo!(),
            types::rules::functions::Type::StringTrim => string::trim::new(item_conf.args)?,
            types::rules::functions::Type::StringEndsWith => {
                string::ends_with::new(item_conf.args)?
            }
            types::rules::functions::Type::StringStartsWith => {
                string::starts_with::new(item_conf.args)?
            }
            types::rules::functions::Type::StringIndexOf => string::index_of::new(item_conf.args)?,
            types::rules::functions::Type::StringLastIndexOf => {
                string::last_index_of::new(item_conf.args)?
            }
            types::rules::functions::Type::StringNumbytes => string::numbytes::new(item_conf.args)?,
            types::rules::functions::Type::StringRegexMatch => {
                string::regex_match::new(item_conf.args)?
            }
            types::rules::functions::Type::StringConcat => string::concat::new(item_conf.args)?,
            types::rules::functions::Type::StringSlice => todo!(),
            types::rules::functions::Type::StringPadEnd => string::pad_end::new(item_conf.args)?,
            types::rules::functions::Type::StringPadStart => {
                string::pad_start::new(item_conf.args)?
            }
            types::rules::functions::Type::StringRepeat => string::repeat::new(item_conf.args)?,
            types::rules::functions::Type::StringIncludes => string::includes::new(item_conf.args)?,

            // hash
            types::rules::functions::Type::HashMd5 => hash::md5::new(item_conf.args)?,
            types::rules::functions::Type::HashSha1 => hash::sha1::new(item_conf.args)?,
            types::rules::functions::Type::HashSha224 => hash::sha224::new(item_conf.args)?,
            types::rules::functions::Type::HashSha256 => hash::sha256::new(item_conf.args)?,
            types::rules::functions::Type::HashSha384 => hash::sha384::new(item_conf.args)?,
            types::rules::functions::Type::HashSha512 => hash::sha512::new(item_conf.args)?,
            types::rules::functions::Type::HashHmacSha1 => hash::hmac_sha1::new(item_conf.args)?,
            types::rules::functions::Type::HashHmacSha224 => {
                hash::hmac_sha224::new(item_conf.args)?
            }
            types::rules::functions::Type::HashHmacSha256 => {
                hash::hmac_sha256::new(item_conf.args)?
            }
            types::rules::functions::Type::HashHmacSha384 => {
                hash::hmac_sha384::new(item_conf.args)?
            }
            types::rules::functions::Type::HashHmacSha512 => {
                hash::hmac_sha512::new(item_conf.args)?
            }

            types::rules::functions::Type::Date => todo!(),
            types::rules::functions::Type::ArrayCardinality => todo!(),
            // compress
            types::rules::functions::Type::CompressBrotli => {
                compress::brotli::new_encoder(item_conf.args)?
            }
            types::rules::functions::Type::DecompressBrotli => {
                compress::brotli::new_decoder(item_conf.args)?
            }
            types::rules::functions::Type::CompressDeflate => {
                compress::deflate::new_encoder(item_conf.args)?
            }
            types::rules::functions::Type::DecompressDeflate => {
                compress::deflate::new_decoder(item_conf.args)?
            }
            types::rules::functions::Type::CompressGzip => {
                compress::gzip::new_encoder(item_conf.args)?
            }
            types::rules::functions::Type::DecompressGzip => {
                compress::gzip::new_decoder(item_conf.args)?
            }
            types::rules::functions::Type::CompressLz4 => {
                compress::lz4::new_encoder(item_conf.args)?
            }
            types::rules::functions::Type::DecompressLz4 => {
                compress::lz4::new_decoder(item_conf.args)?
            }
            types::rules::functions::Type::CompressSnappy => {
                compress::snappy::new_encoder(item_conf.args)?
            }
            types::rules::functions::Type::DecompressSnappy => {
                compress::snappy::new_decoder(item_conf.args)?
            }
            types::rules::functions::Type::CompressZlib => {
                compress::zlib::new_encoder(item_conf.args)?
            }
            types::rules::functions::Type::DecompressZlib => {
                compress::zlib::new_decoder(item_conf.args)?
            }
        };
        computers.push(computer);
    }
    Ok(Box::new(Node { computers }))
}

#[async_trait]
impl Function for Node {
    async fn call(&mut self, message_batch: &mut message::MessageBatch) -> bool {
        let messages = message_batch.get_messages_mut();
        messages.iter_mut().for_each(|msg| {
            self.computers.iter_mut().for_each(|computer| {
                computer.compute(msg);
            });
        });

        true
    }
}
