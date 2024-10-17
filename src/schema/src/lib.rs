use anyhow::Result;
use bytes::Bytes;
use common::{
    error::{HaliaError, HaliaResult},
    get_id, storage,
};
use message::MessageBatch;
use types::{
    schema::{CreateUpdateSchemaReq, QueryParams, SearchSchemasResp},
    Pagination,
};

pub mod decoders;
pub mod encoders;

pub enum Schema {
    Json,
    Csv,
    Avro,
    Yaml,
    Toml,
}

pub trait Decoder {
    fn decode(&self, data: Bytes) -> Result<MessageBatch>;
}

pub trait Encoder {
    fn encode(&self, mb: MessageBatch) -> Result<Bytes>;
}

pub async fn create(req: CreateUpdateSchemaReq) -> HaliaResult<()> {
    if storage::schema::insert_name_exists(&req.base.name).await? {
        return Err(HaliaError::NameExists);
    }

    validate_conf(&req)?;

    let id = get_id();
    storage::schema::insert(&id, req).await?;
    Ok(())
}

pub async fn search(
    pagination: Pagination,
    query_params: QueryParams,
) -> HaliaResult<SearchSchemasResp> {
    todo!()
}

pub async fn update(id: String, req: CreateUpdateSchemaReq) -> HaliaResult<()> {
    if storage::schema::update_name_exists(&id, &req.base.name).await? {
        return Err(HaliaError::NameExists);
    }

    validate_conf(&req)?;
    Ok(())
}

pub async fn delete(id: String) -> HaliaResult<()> {
    todo!()
}

fn validate_conf(req: &CreateUpdateSchemaReq) -> Result<()> {
    match (&req.typ, &req.protocol) {
        (types::schema::SchemaType::Code, types::schema::ProtocolType::Avro) => {
            todo!()
        }
        (types::schema::SchemaType::Code, types::schema::ProtocolType::Protobuf) => todo!(),
        (types::schema::SchemaType::Code, types::schema::ProtocolType::Csv) => todo!(),
        (types::schema::SchemaType::Decode, types::schema::ProtocolType::Avro) => {
            decoders::avro::validate_conf(&req.ext)?;
        }
        (types::schema::SchemaType::Decode, types::schema::ProtocolType::Protobuf) => {
            decoders::protobuf::validate_conf(&req.ext)?;
        }
        (types::schema::SchemaType::Decode, types::schema::ProtocolType::Csv) => {
            decoders::csv::validate_conf(&req.ext)?;
        }
    }
    Ok(())
}
