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
        ListReferencesItem, ListReferencesResp, ListSchemasItem, ListSchemasResp, QueryParams,
        ReadSchemaResp, TemplateEncodeConf,
    },
    Pagination,
};

mod decoders;
mod encoders;

const DEFAULT_KEY: &str = "key";

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
        let reference_cnt = storage::schema::reference::count_by_schema_id(&db_schema.id).await?;
        list.push(ListSchemasItem {
            id: db_schema.id,
            name: db_schema.name,
            schema_type: db_schema.schema_type,
            protocol_type: db_schema.protocol_type,
            reference_cnt,
        });
    }

    Ok(ListSchemasResp { count, list })
}

pub async fn read(id: String) -> HaliaResult<ReadSchemaResp> {
    let db_schema = storage::schema::read_one(&id).await?;
    let reference_cnt = storage::schema::reference::count_by_schema_id(&db_schema.id).await?;
    Ok(ReadSchemaResp {
        id,
        name: db_schema.name,
        schema_type: db_schema.schema_type,
        protocol_type: db_schema.protocol_type,
        conf: db_schema.conf,
        reference_cnt,
        can_delete: reference_cnt == 0,
    })
}

pub async fn update(id: String, req: CreateUpdateSchemaReq) -> HaliaResult<()> {
    validate_conf(&req)?;
    storage::schema::update(&id, req).await?;
    Ok(())
}

pub async fn delete(id: String) -> HaliaResult<()> {
    storage::schema::delete_by_id(&id).await?;
    Ok(())
}

pub async fn list_references(
    id: String,
    pagination: Pagination,
) -> HaliaResult<ListReferencesResp> {
    let (count, references) = storage::schema::reference::query(&id, pagination).await?;
    let mut list = vec![];
    for reference in references {
        let (parent_name, resource_name) = match &reference.parent_type {
            types::schema::ParentType::App => {
                let app = storage::app::read_one(&reference.parent_id).await?;
                let source = storage::app::source_sink::read_one(&reference.resource_id).await?;
                (app.name, source.name)
            }
            types::schema::ParentType::Device => {
                let device = storage::device::device::read_one(&reference.parent_id).await?;
                let source = storage::device::source_sink::read_one(&reference.resource_id).await?;
                (device.name, source.name)
            }
        };
        list.push(ListReferencesItem {
            parent_type: reference.parent_type,
            parent_id: reference.parent_id,
            parent_name,
            resource_type: reference.resource_type,
            resource_id: reference.resource_id,
            resource_name,
        });
    }
    Ok(ListReferencesResp { count, list })
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
        DecodeType::Csv => match schema_id {
            Some(schema_id) => {
                let conf = storage::schema::read_conf(schema_id).await?;
                let conf: CsvDecodeConf = serde_json::from_slice(&conf)?;
                decoders::csv::new_with_conf(conf)
            }
            None => decoders::csv::new(),
        },
        DecodeType::Avro => match schema_id {
            Some(schema_id) => {
                let conf = storage::schema::read_conf(schema_id).await?;
                let conf: AvroDecodeConf = serde_json::from_slice(&conf)?;
                decoders::avro::new_with_conf(conf)
            }
            None => decoders::avro::new(),
        },
        DecodeType::Yaml => todo!(), // decoders::yaml::new(),
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

pub async fn reference_app_source(
    schema_id: &String,
    app_id: &String,
    app_source_id: &String,
) -> HaliaResult<()> {
    storage::schema::reference::insert_app_source(schema_id, app_id, app_source_id).await
}
