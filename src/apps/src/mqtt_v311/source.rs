use common::error::{HaliaError, HaliaResult};
use message::RuleMessageBatch;
use rumqttc::valid_filter;
use schema::Decoder;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use types::apps::mqtt_client_v311::SourceConf;

pub struct Source {
    pub source_conf: SourceConf,
    pub mb_txs: Vec<UnboundedSender<RuleMessageBatch>>,
    pub decoder: Box<dyn Decoder>,
}

impl Source {
    pub async fn new(source_conf: SourceConf) -> Self {
        let decoder = schema::new_decoder(&source_conf.decode_type, &source_conf.schema_id)
            .await
            .unwrap();

        Source {
            source_conf,
            mb_txs: vec![],
            decoder,
        }
    }

    pub async fn process_conf(
        app_id: &String,
        source_id: &String,
        conf: &SourceConf,
    ) -> HaliaResult<()> {
        if !valid_filter(&conf.topic) {
            return Err(HaliaError::Common("topic错误！".to_owned()));
        }

        match conf.decode_type {
            types::schema::DecodeType::Protobuf => match &conf.schema_id {
                Some(schema_id) => {
                    schema::reference_app_source(schema_id, app_id, source_id).await?
                }
                None => return Err(HaliaError::Common("请填写schema_id".to_owned())),
            },
            types::schema::DecodeType::Csv | types::schema::DecodeType::Avro => {
                match &conf.schema_id {
                    Some(schema_id) => {
                        schema::reference_app_source(schema_id, app_id, source_id).await?
                    }
                    None => {}
                }
            }
            types::schema::DecodeType::Raw
            | types::schema::DecodeType::Yaml
            | types::schema::DecodeType::Json
            | types::schema::DecodeType::Toml => {}
        }

        Ok(())
    }

    pub fn get_rxs(&mut self, cnt: usize) -> Vec<UnboundedReceiver<RuleMessageBatch>> {
        let mut rxs = vec![];
        for _ in 0..cnt {
            let (tx, rx) = unbounded_channel();
            self.mb_txs.push(tx);
            rxs.push(rx);
        }
        rxs
    }
}
