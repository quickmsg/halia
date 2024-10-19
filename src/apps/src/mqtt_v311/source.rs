use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use rumqttc::valid_filter;
use schema::Decoder;
use tokio::sync::broadcast;
use types::apps::mqtt_client_v311::SourceConf;

pub struct Source {
    pub conf: SourceConf,
    pub mb_tx: broadcast::Sender<MessageBatch>,
    pub decoder: Box<dyn Decoder>,
}

impl Source {
    pub async fn new(conf: SourceConf) -> Self {
        let decoder = match &conf.decode_type {
            types::apps::mqtt_client_v311::DecodeType::Raw => schema::decoders::raw::Raw::new(),
            types::apps::mqtt_client_v311::DecodeType::Json => schema::decoders::json::Json::new(),
            types::apps::mqtt_client_v311::DecodeType::Csv => schema::decoders::csv::Csv::new(),
            types::apps::mqtt_client_v311::DecodeType::CsvWithSchema => {
                schema::decoders::csv::Csv::new_with_conf(&conf.schema_id.as_ref().unwrap())
                    .await
                    .unwrap()
            }
            types::apps::mqtt_client_v311::DecodeType::Avro => schema::decoders::avro::Avro::new(),
            types::apps::mqtt_client_v311::DecodeType::AvroWithSchema => {
                schema::decoders::avro::Avro::new_with_conf(&conf.schema_id.as_ref().unwrap())
                    .await
                    .unwrap()
            }
            types::apps::mqtt_client_v311::DecodeType::Yaml => todo!(),
            types::apps::mqtt_client_v311::DecodeType::Toml => todo!(),
            types::apps::mqtt_client_v311::DecodeType::Protobuf => todo!(),
        };
        let (mb_tx, _) = broadcast::channel(16);
        Source {
            conf,
            mb_tx,
            decoder,
        }
    }

    pub async fn validate_conf(conf: &SourceConf) -> HaliaResult<()> {
        if !valid_filter(&conf.topic) {
            return Err(HaliaError::Common("topic错误！".to_owned()));
        }

        match conf.decode_type {
            types::apps::mqtt_client_v311::DecodeType::CsvWithSchema
            | types::apps::mqtt_client_v311::DecodeType::AvroWithSchema
            | types::apps::mqtt_client_v311::DecodeType::Protobuf => match &conf.schema_id {
                Some(schema_id) => {
                    storage::schema::add_rc(schema_id).await?;
                }
                None => {
                    return Err(HaliaError::Common("请填写schema_id".to_owned()));
                }
            },
            _ => {}
        }

        Ok(())
    }
}
