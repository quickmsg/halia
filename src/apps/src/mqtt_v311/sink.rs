use std::{collections::HashSet, sync::Arc};

use common::{
    error::{HaliaError, HaliaResult},
    sink_message_retain::{self, SinkMessageRetain},
};
use message::{Message, MessageBatch, RuleMessageBatch};
use regex::Regex;
use rumqttc::{valid_topic, AsyncClient};
use schema::Encoder;
use tokio::{
    select,
    sync::{
        broadcast,
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch,
    },
    task::JoinHandle,
};
use tracing::warn;
use types::apps::mqtt_client_v311::SinkConf;

use super::transfer_qos;

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<JoinHandleData>>,
    pub mb_tx: UnboundedSender<RuleMessageBatch>,
}

pub(crate) struct JoinHandleData {
    topic: Topic,
    pub conf: SinkConf,
    pub encoder: Box<dyn Encoder>,
    pub message_retainer: Box<dyn SinkMessageRetain>,
    pub stop_signal_rx: watch::Receiver<()>,
    pub mb_rx: UnboundedReceiver<RuleMessageBatch>,
    pub mqtt_client: Arc<AsyncClient>,
    pub _app_err_rx: broadcast::Receiver<bool>,
}

impl Sink {
    pub fn validate_conf(conf: &SinkConf) -> HaliaResult<()> {
        if !valid_topic(&conf.topic) {
            return Err(HaliaError::Common("topic不合法！".to_owned()));
        }

        Ok(())
    }

    pub async fn new(
        conf: SinkConf,
        mqtt_client: Arc<AsyncClient>,
        app_err_rx: broadcast::Receiver<bool>,
    ) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = unbounded_channel();
        let encoder = schema::new_encoder(&conf.encode_type, &conf.schema_id)
            .await
            .unwrap();

        let message_retainer = sink_message_retain::new(&conf.message_retain);
        let topic = Topic::new(&conf.topic);
        let join_handle_data = JoinHandleData {
            topic,
            conf,
            encoder,
            message_retainer,
            stop_signal_rx,
            mb_rx,
            mqtt_client,
            _app_err_rx: app_err_rx,
        };

        let join_handle = Self::event_loop(join_handle_data);

        Self {
            mb_tx,
            stop_signal_tx,
            join_handle: Some(join_handle),
        }
    }

    fn event_loop(mut join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        let mut err = false;
        let qos = transfer_qos(&join_handle_data.conf.qos);
        tokio::spawn(async move {
            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        return join_handle_data;
                    }

                    Some(mb) = join_handle_data.mb_rx.recv() => {
                        let mb = mb.take_mb();
                        Self::handle_data(mb, &join_handle_data, qos).await;
                    }
                        // } else {
                        //     join_handle_data.message_retainer.push(mb);
                        // }
                    // }
                }
            }
        })
    }

    async fn handle_data(mb: MessageBatch, join_handle_data: &JoinHandleData, qos: rumqttc::QoS) {
        let topic = {
            let messages = mb.get_messages();
            if messages.len() == 0 {
                return;
            }
            join_handle_data.topic.get_topic(&messages[0])
        };

        let payload = {
            match join_handle_data.encoder.encode(mb) {
                Ok(data) => data,
                Err(e) => {
                    warn!("{:?}", e);
                    return;
                }
            }
        };

        if let Err(e) = join_handle_data
            .mqtt_client
            .publish_bytes(topic, qos, join_handle_data.conf.retain, payload)
            .await
        {
            warn!("{:?}", e);
        }
    }

    pub async fn update_conf(
        &mut self,
        _old_conf: SinkConf,
        new_conf: SinkConf,
    ) -> HaliaResult<()> {
        let mut join_handle_data = self.stop().await;
        join_handle_data.conf = new_conf;
        Self::event_loop(join_handle_data);

        Ok(())
    }

    pub async fn stop(&mut self) -> JoinHandleData {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }

    pub async fn update_mqtt_client(&mut self, mqtt_client: Arc<AsyncClient>) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.mqtt_client = mqtt_client;
        Self::event_loop(join_handle_data);
    }

    pub fn get_txs(&self, cnt: usize) -> Vec<UnboundedSender<RuleMessageBatch>> {
        let mut txs = vec![];
        for _ in 0..cnt {
            txs.push(self.mb_tx.clone());
        }
        txs
    }
}

enum Topic {
    Const(ConstTopic),
    Dynamic(DynamicTopic),
}

struct ConstTopic(String);

struct DynamicTopic {
    template: String,
    fields: Vec<String>,
}

impl DynamicTopic {}

impl Topic {
    fn new(topic: &str) -> Self {
        let re = Regex::new(r"\$\{(.*?)\}").unwrap();
        let mut fields = HashSet::new();
        for cap in re.captures_iter(topic) {
            fields.insert(cap[0][2..cap[0].len() - 1].to_owned());
        }

        if fields.is_empty() {
            Self::Const(ConstTopic(topic.to_owned()))
        } else {
            Self::Dynamic(DynamicTopic {
                template: topic.to_owned(),
                fields: fields.into_iter().collect(),
            })
        }
    }

    fn get_topic(&self, msg: &Message) -> String {
        match self {
            Self::Const(ConstTopic(topic)) => topic.clone(),
            Self::Dynamic(DynamicTopic { template, fields }) => {
                let mut topic = template.clone();
                for field in fields {
                    match msg.get(field) {
                        Some(value) => {
                            topic = topic.replace(&format!("${{{}}}", field), &value.to_string());
                        }
                        None => panic!("todo"),
                    };
                }
                topic
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic() {
        let mut msg = Message::default();
        msg.add(
            "a".to_owned(),
            message::MessageValue::String("hello".to_owned()),
        );
        msg.add(
            "b".to_owned(),
            message::MessageValue::String("world".to_owned()),
        );

        let topic = "hello/world";
        let topic = Topic::new(topic);
        match &topic {
            Topic::Const(ConstTopic(topic)) => assert_eq!(topic, "hello/world"),
            _ => panic!("todo"),
        }
        assert_eq!(topic.get_topic(&msg), "hello/world");

        let topic = "${a}/${b}";
        let topic = Topic::new(topic);
        match &topic {
            Topic::Dynamic(dynamic_topic) => {
                assert_eq!(dynamic_topic.template, "${a}/${b}");
                assert_eq!(dynamic_topic.fields.len(), 2);
                assert!(dynamic_topic.fields.contains(&"a".to_owned()));
                assert!(dynamic_topic.fields.contains(&"b".to_owned()));
            }
            _ => panic!("not right"),
        }
        assert_eq!(topic.get_topic(&msg), "hello/world");
    }
}
