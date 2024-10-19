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
        let decoder = schema::new_decoder(&conf.decode_type, &conf.schema_id)
            .await
            .unwrap();
        let (mb_tx, _) = broadcast::channel(16);
        Source {
            conf,
            mb_tx,
            decoder,
        }
    }

    pub async fn process_conf(id: &String, conf: &SourceConf) -> HaliaResult<()> {
        if !valid_filter(&conf.topic) {
            return Err(HaliaError::Common("topic错误！".to_owned()));
        }

        match conf.decode_type {
            types::schema::DecodeType::CsvWithSchema
            | types::schema::DecodeType::AvroWithSchema
            | types::schema::DecodeType::Protobuf => match &conf.schema_id {
                Some(schema_id) => {
                    schema::reference(schema::ResourceType::Device, schema_id, id).await?
                }
                None => return Err(HaliaError::Common("请填写schema_id".to_owned())),
            },
            _ => {}
        }

        Ok(())
    }
}
