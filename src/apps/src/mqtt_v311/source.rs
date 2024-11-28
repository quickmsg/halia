use std::sync::Arc;

use common::error::{HaliaError, HaliaResult};
use futures::lock::BiLock;
use halia_derive::SourceErr;
use message::RuleMessageBatch;
use rumqttc::valid_filter;
use schema::Decoder;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use types::apps::mqtt_client_v311::SourceConf;

#[derive(SourceErr)]
pub struct Source {
    pub conf: SourceConf,
    pub mb_txs: Vec<UnboundedSender<RuleMessageBatch>>,
    pub decoder: Box<dyn Decoder>,
    pub err: BiLock<Option<Arc<String>>>,
}

impl Source {
    pub async fn new(conf: SourceConf) -> Self {
        let decoder = schema::new_decoder(&conf.decode_type, &conf.schema_id)
            .await
            .unwrap();
        Source {
            conf,
            mb_txs: vec![],
            decoder,
            err: todo!(),
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
            types::schema::DecodeType::CsvWithSchema
            | types::schema::DecodeType::AvroWithSchema
            | types::schema::DecodeType::Protobuf => match &conf.schema_id {
                Some(schema_id) => {
                    schema::reference_app_source(schema_id, app_id, source_id).await?
                }
                None => return Err(HaliaError::Common("请填写schema_id".to_owned())),
            },
            _ => {}
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
