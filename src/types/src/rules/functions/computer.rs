use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Conf {
    pub items: Vec<ItemConf>,
}

#[derive(Deserialize, Serialize)]
pub struct ItemConf {
    #[serde(rename = "type")]
    pub typ: Type,
    pub number: Option<NumberItemConf>,
    pub string: Option<StringItemConf>,
    pub hash: Option<HashItemConf>,
    pub array: Option<ArrayItemConf>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Number,
    String,
    Hash,
    Date,
    Array,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct NumberItemConf {
    #[serde(rename = "type")]
    pub typ: NumberType,
    pub field: String,
    pub target_field: Option<String>,
    pub args: Option<Vec<serde_json::Value>>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NumberType {
    Abs,
    Acos,
    Acosh,
    Add,
    Asin,
    Asinh,
    Atan,
    Atan2,
    Atanh,
    Bitand,
    Bitnot,
    Bitor,
    Bitxor,
    Cbrt,
    Ceil,
    Cos,
    Cosh,
    Degrees,
    Exp,
    Exp2,
    Floor,
    Ln,
    Log,
    Power,
    Sin,
}

#[derive(Deserialize, Serialize)]
pub struct HashItemConf {
    #[serde(rename = "type")]
    pub typ: HashType,
    pub field: String,
    pub target_field: Option<String>,
    pub args: Option<Vec<serde_json::Value>>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HashType {
    Md5,
    Sha1,
    Sha256,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct StringItemConf {
    pub typ: StringType,
    pub field: String,
    pub target_field: Option<String>,
    pub args: Option<Vec<serde_json::Value>>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StringType {
    Base64,
    Hex,
    Length,
    Lower,
    Upper,
    Ltrim,
    Lpad,
    Reverse,
    Rtrim,
    Split,
    Trim,
    Endswith,
    Indexof,
    Numbytes,
    RegexMatch,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ArrayItemConf {
    #[serde(rename = "type")]
    pub typ: ArrayType,
    pub field: String,
    pub target_field: Option<String>,
    pub args: Option<Vec<serde_json::Value>>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArrayType {
    Cardinality,
}
