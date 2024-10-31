use common::error::{HaliaError, HaliaResult};
use message::RuleMessageBatch;
use rumqttc::valid_filter;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use types::apps::mqtt_client_v50::SourceConf;

pub struct Source {
    pub conf: SourceConf,
    pub mb_txs: Vec<UnboundedSender<RuleMessageBatch>>,
}

impl Source {
    pub fn new(conf: SourceConf) -> Self {
        Source {
            conf,
            mb_txs: vec![],
        }
    }

    pub fn validate_conf(conf: &SourceConf) -> HaliaResult<()> {
        if !valid_filter(&conf.topic) {
            return Err(HaliaError::Common("topic错误！".to_owned()));
        }

        Ok(())
    }

    pub fn get_rx(&mut self) -> UnboundedReceiver<RuleMessageBatch> {
        let (tx, rx) = unbounded_channel();
        self.mb_txs.push(tx);
        rx
    }
}
