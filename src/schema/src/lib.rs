use anyhow::Result;
use bytes::Bytes;
use common::{
    error::{HaliaError, HaliaResult},
    get_id, vec_to_string,
};
use message::MessageBatch;
use types::{
    schema::{CreateUpdateSchemaReq, QueryParams, SearchSchemasItemResp, SearchSchemasResp},
    BaseConf, Pagination,
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
    validate_conf(&req)?;

    let id = get_id();
    storage::schema::insert(&id, req).await?;
    Ok(())
}

pub async fn search(
    pagination: Pagination,
    query_params: QueryParams,
) -> HaliaResult<SearchSchemasResp> {
    let (count, db_schemas) = storage::schema::search(pagination, query_params).await?;

    let mut resp_schemas = vec![];
    for db_schema in db_schemas {
        resp_schemas.push(transfer_db_schema_to_resp(db_schema)?);
    }

    Ok(SearchSchemasResp {
        total: count,
        data: resp_schemas,
    })
}

pub async fn update(id: String, req: CreateUpdateSchemaReq) -> HaliaResult<()> {
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

fn transfer_db_schema_to_resp(
    db_schema: storage::schema::Schema,
) -> HaliaResult<SearchSchemasItemResp> {
    Ok(SearchSchemasItemResp {
        conf: CreateUpdateSchemaReq {
            typ: types::schema::SchemaType::try_from(db_schema.typ)?,
            protocol: types::schema::ProtocolType::try_from(db_schema.protocol_type)?,
            base: BaseConf {
                name: db_schema.name,
                desc: vec_to_string(db_schema.des),
            },
            ext: serde_json::from_slice(&db_schema.conf)?,
        },
    })
}
