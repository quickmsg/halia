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
    pub args: HashMap<String, serde_json::Value>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    // number
    NumberAbs,
    NumberAdd,
    NumberBitand,
    NumberBitnot,
    NumberBitor,
    NumberBitxor,
    NumberCbrt,
    NumberCeil,
    NumberDegrees,
    NumberExp,
    NumberExp2,
    NumberFloor,
    NumberLn,
    NumberLog,
    NumberPow,
    NumberSub,
    NumberMulti,
    NumberDivision,
    NumberModulo,
    NumberRandom,
    NumberRound,
    NumberSgn,

    // 三角函数
    TrigonometricAcos,
    TrigonometricAcosh,
    TrigonometricAsin,
    TrigonometricAsinh,
    TrigonometricAtan,
    TrigonometricAtan2,
    TrigonometricAtanh,
    TrigonometricSin,
    TrigonometricCos,
    TrigonometricCosh,

    // string
    StringNew,
    StringConcat,
    StringEndsWith,
    StringIndexOf,
    StringStartsWith,
    StringBase64,
    StringHex,
    StringLength,
    StringLower,
    StringUpper,
    StringTrimStart,
    StringReverse,
    StringTrimEnd,
    StringSplit,
    StringTrim,
    StringPadStart,
    StringPadEnd,
    StringNumbytes,
    StringLastIndexOf,
    StringRegexMatch,
    StringSlice,
    StringRepeat,
    StringIncludes,

    // hash
    HashMd5,
    HashSha1,
    HashSha224,
    HashSha256,
    HashSha384,
    HashSha512,
    HashHmacSha1,
    HashHmacSha224,
    HashHmacSha256,
    HashHmacSha384,
    HashHmacSha512,

    //
    Date,

    // 数组
    ArrayLen,
    ArrayPush,
    ArrayPop,
    ArrayJoin,
    ArrayDistinct,
    ArrayReverse,
    ArrayIndexOf,
    ArrayLastIndexOf,

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

    // 类型判断
    TypeJudgmentArray,
    TypeJudgmentBool,
    TypeJudgmentFloat,
    TypeJudgmentInt,
    TypeJudgmentNull,
    TypeJudgmentString,
    TypeJudgmentObject,

    // 类型转换
    TypeConversionBool,
    TypeConversionFloat,
    TypeConversionInt,
    TypeConversionStr,
}
