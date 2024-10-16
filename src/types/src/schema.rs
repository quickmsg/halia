use serde::{Deserialize, Serialize};

use crate::BaseConf;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateSchemaReq {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: SchemaConf,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SchemaConf {}

pub enum SchemaType {
    Avro,
    Protobuf,
    Csv,
}
