use anyhow::Result;
use bytes::Bytes;
use common::{
    error::{HaliaError, HaliaResult},
    get_id,
};
use message::MessageBatch;
use types::{
    schema::{
        AvroDecodeConf, CreateUpdateSchemaReq, CsvDecodeConf, DecodeType, EncodeType,
        ListSchemasItem, ListSchemasResp, QueryParams, ReadSchemaResp, TemplateEncodeConf,
    },
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

pub trait Decoder: Sync + Send {
    fn decode(&self, data: Bytes) -> Result<MessageBatch>;
}

pub trait Encoder: Sync + Send {
    fn encode(&self, mb: MessageBatch) -> Result<Bytes>;
}

pub async fn create(req: CreateUpdateSchemaReq) -> HaliaResult<()> {
    validate_conf(&req)?;

    let id = get_id();
    storage::schema::insert(&id, req).await?;
    Ok(())
}

pub async fn list(
    pagination: Pagination,
    query_params: QueryParams,
) -> HaliaResult<ListSchemasResp> {
    let (count, db_schemas) = storage::schema::search(pagination, query_params).await?;

    let mut list = vec![];
    for db_schema in db_schemas {
        list.push(ListSchemasItem {
            id: db_schema.id,
            name: db_schema.name,
            schema_type: db_schema.schema_type,
            protocol_type: db_schema.protocol_type,
            // TODO
            refrence_cnt: 0,
            // TODO
            can_delete: false,
        });
    }

    Ok(ListSchemasResp { count, list })
}

pub async fn read(id: String) -> HaliaResult<ReadSchemaResp> {
    let db_schema = storage::schema::read_one(&id).await?;
    Ok(ReadSchemaResp {
        id,
        name: db_schema.name,
        schema_type: db_schema.schema_type,
        protocol_type: db_schema.protocol_type,
        conf: db_schema.conf,
        // TODO
        refrence_cnt: 0,
        // TODO
        can_delete: false,
    })
}

pub async fn update(id: String, req: CreateUpdateSchemaReq) -> HaliaResult<()> {
    validate_conf(&req)?;
    storage::schema::update(&id, req).await?;
    Ok(())
}

pub async fn delete(id: String) -> HaliaResult<()> {
    storage::schema::delete_by_id(&id).await?;
    todo!()
}

fn validate_conf(req: &CreateUpdateSchemaReq) -> HaliaResult<()> {
    match &req.schema_type {
        types::schema::SchemaType::Encode => match &req.protocol_type {
            types::schema::ProtocolType::Avro => todo!(),
            types::schema::ProtocolType::Protobuf => todo!(),
            types::schema::ProtocolType::Csv => todo!(),
            types::schema::ProtocolType::Template => {
                encoders::template::validate_conf(&req.conf)?;
            }
        },
        types::schema::SchemaType::Decode => match &req.protocol_type {
            types::schema::ProtocolType::Avro => {
                decoders::avro::validate_conf(&req.conf)?;
            }
            types::schema::ProtocolType::Protobuf => {
                decoders::protobuf::validate_conf(&req.conf)?;
            }
            types::schema::ProtocolType::Csv => {
                decoders::csv::validate_conf(&req.conf)?;
            }
            types::schema::ProtocolType::Template => {
                return Err(HaliaError::NotSupportResource);
            }
        },
    }

    Ok(())
}

pub async fn new_decoder(
    decode_type: &DecodeType,
    schema_id: &Option<String>,
) -> HaliaResult<Box<dyn Decoder>> {
    match decode_type {
        DecodeType::Raw => decoders::raw::new(),
        DecodeType::Json => decoders::json::new(),
        DecodeType::Csv => decoders::csv::new(),
        DecodeType::CsvWithSchema => match schema_id {
            Some(schema_id) => {
                let conf = storage::schema::read_conf(schema_id).await?;
                let conf: CsvDecodeConf = serde_json::from_slice(&conf)?;
                decoders::csv::new_with_conf(conf)
            }
            None => return Err(HaliaError::Common("必须提供schema_id".to_owned())),
        },
        DecodeType::Avro => decoders::avro::new(),
        DecodeType::AvroWithSchema => match schema_id {
            Some(schema_id) => {
                let conf = storage::schema::read_conf(schema_id).await?;
                let conf: AvroDecodeConf = serde_json::from_slice(&conf)?;
                decoders::avro::new_with_conf(conf)
            }
            None => return Err(HaliaError::Common("必须提供schema_id".to_owned())),
        },
        DecodeType::Yaml => todo!(),
        DecodeType::Toml => todo!(),
        DecodeType::Protobuf => todo!(),
    }
}

pub async fn new_encoder(
    encode_type: &EncodeType,
    schema_id: &Option<String>,
) -> HaliaResult<Box<dyn Encoder>> {
    match encode_type {
        EncodeType::Template => match schema_id {
            Some(schema_id) => {
                let conf = storage::schema::read_conf(schema_id).await?;
                let conf: TemplateEncodeConf = serde_json::from_slice(&conf)?;
                encoders::template::new(conf.template)
            }
            None => return Err(HaliaError::Common("必须提供schema_id".to_owned())),
        },
        EncodeType::Json => encoders::json::new(),
    }
}

pub enum ResourceType {
    Device,
    App,
}

pub async fn reference(
    _rt: ResourceType,
    schema_id: &String,
    resource_id: &String,
) -> HaliaResult<()> {
    storage::schema::reference::insert(schema_id, resource_id).await
}
