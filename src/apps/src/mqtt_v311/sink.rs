use std::{
    collections::HashSet,
    sync::{atomic::AtomicBool, Arc},
};

use common::{
    error::{HaliaError, HaliaResult},
    sink_message_retain::{self, SinkMessageRetain},
};
use halia_derive::{ResourceStop, SinkTxs};
use message::{Message, RuleMessageBatch};
use regex::Regex;
use rumqttc::{valid_topic, AsyncClient};
use schema::Encoder;
use tokio::{
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch,
    },
    task::JoinHandle,
};
use tracing::warn;
use types::apps::mqtt_client_v311::SinkConf;

use super::transfer_qos;

#[derive(ResourceStop, SinkTxs)]
pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<TaskLoop>>,
    mb_tx: UnboundedSender<RuleMessageBatch>,
}

pub struct TaskLoop {
    topic: Topic,
    sink_conf: SinkConf,
    qos: rumqttc::QoS,
    pub encoder: Box<dyn Encoder>,
    pub message_retainer: Box<dyn SinkMessageRetain>,
    pub stop_signal_rx: watch::Receiver<()>,
    pub mb_rx: UnboundedReceiver<RuleMessageBatch>,
    mqtt_client: Arc<AsyncClient>,
    mqtt_status: Arc<AtomicBool>,
}

impl TaskLoop {
    pub async fn new(
        sink_conf: SinkConf,
        stop_signal_rx: watch::Receiver<()>,
        mb_rx: UnboundedReceiver<RuleMessageBatch>,
        mqtt_client: Arc<AsyncClient>,
        mqtt_status: Arc<AtomicBool>,
    ) -> Self {
        let qos = transfer_qos(&sink_conf.qos);
        let encoder = schema::new_encoder(&sink_conf.encode_type, &sink_conf.schema_id)
            .await
            .unwrap();
        let message_retainer = sink_message_retain::new(&sink_conf.message_retain);
        let topic = Topic::new(&sink_conf.topic);
        Self {
            topic,
            sink_conf,
            qos,
            encoder,
            message_retainer,
            stop_signal_rx,
            mb_rx,
            mqtt_client,
            mqtt_status,
        }
    }

    pub fn start(mut self) -> JoinHandle<Self> {
        tokio::spawn(async move {
            loop {
                select! {
                    _ = self.stop_signal_rx.changed() => {
                        return self;
                    }

                    Some(mb) = self.mb_rx.recv() => {
                        self.handle_data(mb).await;
                    }
                }
            }
        })
    }

    async fn handle_data(&mut self, rmb: RuleMessageBatch) {
        let mb = rmb.take_mb();
        if !self.mqtt_status.load(std::sync::atomic::Ordering::Relaxed) {
            self.message_retainer.push(mb);
            return;
        }
        let topic = {
            let messages = mb.get_messages();
            if messages.len() == 0 {
                return;
            }
            self.topic.get_topic(&messages[0])
        };

        let payload = {
            match self.encoder.encode(mb) {
                Ok(data) => data,
                Err(e) => {
                    warn!("{:?}", e);
                    return;
                }
            }
        };

        self.mqtt_client
            .publish_bytes(topic, self.qos, self.sink_conf.retain, payload)
            .await
            .unwrap();
    }
}

impl Sink {
    pub fn validate_conf(conf: &SinkConf) -> HaliaResult<()> {
        if !valid_topic(&conf.topic) {
            return Err(HaliaError::Common("topic不合法！".to_owned()));
        }

        Ok(())
    }

    pub async fn new(
        sink_conf: SinkConf,
        mqtt_client: Arc<AsyncClient>,
        mqtt_status: Arc<AtomicBool>,
    ) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = unbounded_channel();

        let task_loop =
            TaskLoop::new(sink_conf, stop_signal_rx, mb_rx, mqtt_client, mqtt_status).await;
        let join_handle = task_loop.start();

        Self {
            mb_tx,
            stop_signal_tx,
            join_handle: Some(join_handle),
        }
    }

    pub async fn update_conf(&mut self, _old_conf: SinkConf, new_conf: SinkConf) {
        let mut task_loop = self.stop().await;
        task_loop.sink_conf = new_conf;
        let join_handle = task_loop.start();
        self.join_handle = Some(join_handle);
    }

    pub async fn update_mqtt_client(&mut self, mqtt_client: Arc<AsyncClient>) {
        let mut task_loop = self.stop().await;
        task_loop.mqtt_client = mqtt_client;
        let join_handle = task_loop.start();
        self.join_handle = Some(join_handle);
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
