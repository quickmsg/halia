use anyhow::Result;
use async_trait::async_trait;
use message::Message;
use types::rules::functions::Conf;

use crate::{args::Args, Function};

mod array;
mod compress;
mod date;
mod hash;
mod number;
mod string;

pub trait Computer: Sync + Send {
    fn compute(&mut self, message: &mut Message);
}

pub struct Node {
    computers: Vec<Box<dyn Computer>>,
}

pub fn validate_conf(conf: Conf) -> Result<()> {
    for item_conf in conf.items.into_iter() {
        match item_conf.typ {
            // number
            types::rules::functions::Type::NumberAbs => {
                number::abs::validate_conf(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberAcos => {
                number::acos::validate_conf(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberAcosh => {
                number::acosh::validate_conf(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberAdd => {
                number::add::validate_conf(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberAsin => {
                number::asin::validate_conf(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberAsinh => {
                number::asinh::validate_conf(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberAtan => {
                number::atan::validate_conf(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberAtan2 => {
                // number::atan2::validate_conf(&item_conf)?
                todo!()
            }
            types::rules::functions::Type::NumberAtanh => {
                number::atanh::validate_conf(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberBitand => {
                // number::bitand::validate_conf(&item_conf)?
                todo!()
            }
            types::rules::functions::Type::NumberBitnot => {
                // number::bitnot::validate_conf(&item_conf)?
                todo!()
            }
            types::rules::functions::Type::NumberBitor => {
                // number::bitor::validate_conf(&item_conf)?
                todo!()
            }
            types::rules::functions::Type::NumberBitxor => {
                // number::bitxor::validate_conf(&item_conf)?
                todo!()
            }
            types::rules::functions::Type::NumberCbrt => {
                number::cbrt::validate_conf(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberCeil => {
                number::ceil::validate_conf(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberCos => {
                number::cos::validate_conf(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberCosh => todo!(),
            types::rules::functions::Type::NumberDegrees => todo!(),
            types::rules::functions::Type::NumberExp => todo!(),
            types::rules::functions::Type::NumberExp2 => todo!(),
            types::rules::functions::Type::NumberFloor => todo!(),
            types::rules::functions::Type::NumberLn => todo!(),
            types::rules::functions::Type::NumberLog => todo!(),
            types::rules::functions::Type::NumberPower => todo!(),
            types::rules::functions::Type::NumberSin => todo!(),
            types::rules::functions::Type::NumberSub => todo!(),
            types::rules::functions::Type::NumberMulti => todo!(),
            types::rules::functions::Type::NumberDivision => todo!(),
            types::rules::functions::Type::NumberModulo => todo!(),
            types::rules::functions::Type::StringNew => todo!(),
            types::rules::functions::Type::StringConcat => todo!(),
            types::rules::functions::Type::StringEndsWith => todo!(),
            types::rules::functions::Type::StringIndexOf => todo!(),
            types::rules::functions::Type::StringStartsWith => todo!(),
            types::rules::functions::Type::StringBase64 => todo!(),
            types::rules::functions::Type::StringHex => todo!(),
            types::rules::functions::Type::StringLength => todo!(),
            types::rules::functions::Type::StringLower => todo!(),
            types::rules::functions::Type::StringUpper => todo!(),
            types::rules::functions::Type::StringTrimStart => todo!(),
            types::rules::functions::Type::StringReverse => todo!(),
            types::rules::functions::Type::StringTrimEnd => todo!(),
            types::rules::functions::Type::StringSplit => todo!(),
            types::rules::functions::Type::StringTrim => todo!(),
            types::rules::functions::Type::StringPadStart => todo!(),
            types::rules::functions::Type::StringPadEnd => todo!(),
            types::rules::functions::Type::StringNumbytes => todo!(),
            types::rules::functions::Type::StringLastIndexOf => todo!(),
            types::rules::functions::Type::StringRegexMatch => todo!(),
            types::rules::functions::Type::StringSlice => todo!(),
            types::rules::functions::Type::StringRepeat => todo!(),
            types::rules::functions::Type::StringIncludes => todo!(),
            types::rules::functions::Type::HashMd5 => todo!(),
            types::rules::functions::Type::HashSha1 => todo!(),
            types::rules::functions::Type::HashSha224 => todo!(),
            types::rules::functions::Type::HashSha256 => todo!(),
            types::rules::functions::Type::HashSha384 => todo!(),
            types::rules::functions::Type::HashSha512 => todo!(),
            types::rules::functions::Type::HashHmacSha1 => todo!(),
            types::rules::functions::Type::HashHmacSha224 => todo!(),
            types::rules::functions::Type::HashHmacSha256 => todo!(),
            types::rules::functions::Type::HashHmacSha384 => todo!(),
            types::rules::functions::Type::HashHmacSha512 => todo!(),
            types::rules::functions::Type::Date => todo!(),

            types::rules::functions::Type::CompressBrotli => todo!(),
            types::rules::functions::Type::DecompressBrotli => todo!(),
            types::rules::functions::Type::CompressDeflate => todo!(),
            types::rules::functions::Type::DecompressDeflate => todo!(),
            types::rules::functions::Type::CompressGzip => todo!(),
            types::rules::functions::Type::DecompressGzip => todo!(),
            types::rules::functions::Type::CompressLz4 => todo!(),
            types::rules::functions::Type::DecompressLz4 => todo!(),
            types::rules::functions::Type::CompressSnappy => todo!(),
            types::rules::functions::Type::DecompressSnappy => todo!(),
            types::rules::functions::Type::CompressZlib => todo!(),
            types::rules::functions::Type::DecompressZlib => todo!(),
            types::rules::functions::Type::ArrayJoin => todo!(),
            types::rules::functions::Type::ArrayLen => todo!(),
            types::rules::functions::Type::ArrayPush => todo!(),
            types::rules::functions::Type::ArrayPop => todo!(),
            types::rules::functions::Type::ArrayDistinct => todo!(),
            types::rules::functions::Type::ArrayReverse => todo!(),
            types::rules::functions::Type::ArrayIndexOf => todo!(),
            types::rules::functions::Type::ArrayLastIndexOf => todo!(),
        }
    }

    Ok(())
}

pub fn new(conf: Conf) -> Result<Box<dyn Function>> {
    let mut computers: Vec<Box<dyn Computer>> = Vec::with_capacity(conf.items.len());
    for item_conf in conf.items {
        let computer = match item_conf.typ {
            // number
            types::rules::functions::Type::NumberAbs => {
                number::abs::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberAcos => {
                number::acos::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberAcosh => {
                number::acosh::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberAdd => {
                number::add::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberAsin => {
                number::asin::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberAsinh => {
                number::asinh::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberAtan => {
                number::atan::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberAtan2 => {
                //  number::atan2::new(item_conf)?,
                todo!()
            }
            types::rules::functions::Type::NumberAtanh => {
                number::atanh::new(Args::new(item_conf.args))?
            }
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
            types::rules::functions::Type::StringNew => {
                string::new::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberCbrt => {
                number::cbrt::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberCeil => {
                number::ceil::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberCos => {
                number::cos::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberCosh => {
                number::cosh::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberDegrees => {
                number::degrees::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberExp => {
                number::exp::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberExp2 => {
                number::exp2::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberFloor => {
                number::floor::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberLn => number::ln::new(Args::new(item_conf.args))?,
            types::rules::functions::Type::NumberLog => todo!(),
            types::rules::functions::Type::NumberPower => todo!(),
            types::rules::functions::Type::NumberSin => {
                number::sin::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberSub => {
                number::sub::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberMulti => {
                number::multi::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberDivision => {
                number::division::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::NumberModulo => {
                number::modulo::new(Args::new(item_conf.args))?
            }

            // string
            types::rules::functions::Type::StringBase64 => {
                // string::base64::new(item_conf)?
                todo!()
            }
            types::rules::functions::Type::StringHex => {
                // string::hex::new(item_conf)?;
                todo!()
            }
            types::rules::functions::Type::StringLength => {
                string::length::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::StringLower => {
                string::lower::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::StringUpper => {
                string::upper::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::StringTrimStart => {
                string::trim_start::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::StringReverse => {
                string::reverse::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::StringTrimEnd => {
                string::trim_end::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::StringSplit => todo!(),
            types::rules::functions::Type::StringTrim => {
                string::trim::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::StringEndsWith => {
                string::ends_with::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::StringStartsWith => {
                string::starts_with::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::StringIndexOf => {
                string::index_of::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::StringLastIndexOf => {
                string::last_index_of::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::StringNumbytes => {
                string::numbytes::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::StringRegexMatch => {
                string::regex_match::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::StringConcat => {
                string::concat::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::StringSlice => todo!(),
            types::rules::functions::Type::StringPadEnd => {
                string::pad_end::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::StringPadStart => {
                string::pad_start::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::StringRepeat => {
                string::repeat::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::StringIncludes => {
                string::includes::new(Args::new(item_conf.args))?
            }

            // hash
            types::rules::functions::Type::HashMd5 => hash::md5::new(Args::new(item_conf.args))?,
            types::rules::functions::Type::HashSha1 => hash::sha1::new(Args::new(item_conf.args))?,
            types::rules::functions::Type::HashSha224 => {
                hash::sha224::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::HashSha256 => {
                hash::sha256::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::HashSha384 => {
                hash::sha384::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::HashSha512 => {
                hash::sha512::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::HashHmacSha1 => {
                hash::hmac_sha1::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::HashHmacSha224 => {
                hash::hmac_sha224::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::HashHmacSha256 => {
                hash::hmac_sha256::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::HashHmacSha384 => {
                hash::hmac_sha384::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::HashHmacSha512 => {
                hash::hmac_sha512::new(Args::new(item_conf.args))?
            }

            types::rules::functions::Type::Date => todo!(),

            // array
            types::rules::functions::Type::ArrayLen => array::len::new(Args::new(item_conf.args))?,
            types::rules::functions::Type::ArrayPush => {
                array::push::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::ArrayPop => array::pop::new(Args::new(item_conf.args))?,
            types::rules::functions::Type::ArrayJoin => {
                array::join::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::ArrayDistinct => {
                array::distinct::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::ArrayReverse => {
                array::reverse::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::ArrayIndexOf => {
                array::index_of::new(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::ArrayLastIndexOf => {
                array::last_index_of::new(Args::new(item_conf.args))?
            }

            // compress
            types::rules::functions::Type::CompressBrotli => {
                compress::brotli::new_encoder(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::DecompressBrotli => {
                compress::brotli::new_decoder(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::CompressDeflate => {
                compress::deflate::new_encoder(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::DecompressDeflate => {
                compress::deflate::new_decoder(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::CompressGzip => {
                compress::gzip::new_encoder(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::DecompressGzip => {
                compress::gzip::new_decoder(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::CompressLz4 => {
                compress::lz4::new_encoder(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::DecompressLz4 => {
                compress::lz4::new_decoder(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::CompressSnappy => {
                compress::snappy::new_encoder(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::DecompressSnappy => {
                compress::snappy::new_decoder(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::CompressZlib => {
                compress::zlib::new_encoder(Args::new(item_conf.args))?
            }
            types::rules::functions::Type::DecompressZlib => {
                compress::zlib::new_decoder(Args::new(item_conf.args))?
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
