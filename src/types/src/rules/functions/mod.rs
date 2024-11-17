use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub mod aggregation;
pub mod filter;
pub mod window;

#[derive(Deserialize, Serialize)]
pub struct Conf {
    pub items: Vec<ItemConf>,
}

#[derive(Deserialize, Serialize)]
pub struct ItemConf {
    #[serde(rename = "type")]
    pub typ: Type,
    pub field: String,
    pub target_field: Option<String>,
    pub args: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    // number
    NumberAbs,
    NumberAcos,
    NumberAcosh,
    NumberAdd,
    NumberAsin,
    NumberAsinh,
    NumberAtan,
    NumberAtan2,
    NumberAtanh,
    NumberBitand,
    NumberBitnot,
    NumberBitor,
    NumberBitxor,
    NumberCbrt,
    NumberCeil,
    NumberCos,
    NumberCosh,
    NumberDegrees,
    NumberExp,
    NumberExp2,
    NumberFloor,
    NumberLn,
    NumberLog,
    NumberPower,
    NumberSin,
    NumberSub,
    NumberMulti,
    NumberDivision,
    NumberModulo,

    // string
    StringBase64,
    StringHex,
    StringLength,
    StringLower,
    StringUpper,
    StringLtrim,
    StringLpad,
    StringReverse,
    StringRtrim,
    StringSplit,
    StringTrim,
    StringEndsWith,
    StringStartsWith,
    StringIndexOf,
    StringNumbytes,
    StringRegexMatch,
    StringConcat,
    StringSlice,

    // hash
    HashMd5,
    HashSha1,
    HashSha256,

    //
    Date,

    // 数组
    ArrayCardinality,

    // compress
    CompressBrotli,
    DecompressBrotli,
    CompressDeflate,
    DecompressDeflate,
    CompressGzip,
    DecompressGzip,
    CompressLz4,
    DecompressLz4,
    CompressSnappy,
    DecompressSnappy,
    CompressZlib,
    DecompressZlib,
}