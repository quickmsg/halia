use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use rumqttc::valid_filter;
use tokio::sync::broadcast;
use types::apps::mqtt_client::SourceConf;

pub struct Source {
    pub conf: SourceConf,
    pub mb_tx: broadcast::Sender<MessageBatch>,
}

impl Source {
    pub fn new(conf: SourceConf) -> Self {
        let (mb_tx, _) = broadcast::channel(16);
        Source { conf, mb_tx }
    }

    pub fn validate_conf(conf: &SourceConf) -> HaliaResult<()> {
        if !valid_filter(&conf.topic) {
            return Err(HaliaError::Common("topic错误！".to_owned()));
        }

        Ok(())
    }
}