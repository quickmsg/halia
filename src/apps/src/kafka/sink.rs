use std::{collections::BTreeMap, sync::Arc};

use anyhow::Result;
use chrono::{TimeZone, Utc};
use common::{
    error::HaliaResult,
    sink_message_retain::{self, SinkMessageRetain},
};
use futures::lock::BiLock;
use message::RuleMessageBatch;
use rskafka::{
    client::{
        partition::{Compression, PartitionClient, UnknownTopicHandling},
        Client,
    },
    record::Record,
};
use tokio::{
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch,
    },
    task::JoinHandle,
};
use types::apps::kafka::SinkConf;
use utils::ErrorManager;

pub struct Sink {
    err: BiLock<Option<Arc<String>>>,
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<TaskLoop>>,
    pub mb_tx: UnboundedSender<RuleMessageBatch>,
}

pub struct TaskLoop {
    sink_id: String,
    sink_conf: SinkConf,
    partition_client: Option<PartitionClient>,
    app_err_tx: UnboundedSender<Option<Arc<String>>>,
    stop_signal_rx: watch::Receiver<()>,
    mb_rx: UnboundedReceiver<RuleMessageBatch>,
    message_retainer: Box<dyn SinkMessageRetain>,
    error_manager: ErrorManager,
}

impl TaskLoop {
    fn new(
        sink_id: String,
        sink_conf: SinkConf,
        sink_err: BiLock<Option<Arc<String>>>,
        partition_client: Option<PartitionClient>,
        app_err_tx: UnboundedSender<Option<Arc<String>>>,
        stop_signal_rx: watch::Receiver<()>,
        mb_rx: UnboundedReceiver<RuleMessageBatch>,
    ) -> Self {
        let message_retainer = sink_message_retain::new(&sink_conf.message_retain);
        let error_manager = ErrorManager::new(
            utils::error_manager::ResourceType::AppSink,
            sink_id.clone(),
            sink_err,
        );
        Self {
            sink_id,
            sink_conf,
            partition_client,
            app_err_tx,
            stop_signal_rx,
            mb_rx,
            message_retainer,
            error_manager,
        }
    }

    fn start(mut self) -> JoinHandle<Self> {
        let compression = transfer_compression(&self.sink_conf.compression);
        tokio::spawn(async move {
            loop {
                select! {
                    _ = self.stop_signal_rx.changed() => {
                        return self;
                    }

                    Some(mb) = self.mb_rx.recv() => {
                        match &self.partition_client {
                            Some(partition_client) => {
                                &self.send_msg_to_kafka(partition_client, mb, compression).await;
                            }
                            None => {
                                let mb = mb.take_mb();
                                self.message_retainer.push(mb);
                            }
                        }

                    }
                }
            }
        })
    }

    async fn send_msg_to_kafka(
        &self,
        partition_client: &PartitionClient,
        rmb: RuleMessageBatch,
        compression: Compression,
    ) -> Result<()> {
        let mb = rmb.take_mb();
        let key = self.sink_conf.key.clone().map(|value| {
            let value: Vec<u8> = value.into();
            value
        });
        let mut headers = BTreeMap::new();
        if let Some(conf_headers) = &self.sink_conf.headers {
            for (k, v) in conf_headers.iter() {
                let v: Vec<u8> = v.clone().into();
                headers.insert(k.clone(), v);
            }
        }

        let record = Record {
            key,
            value: Some(mb.to_json()),
            headers,
            timestamp: Utc.timestamp_millis_opt(0).unwrap(),
        };
        partition_client.produce(vec![record], compression).await?;
        Ok(())
    }
}

impl Sink {
    pub fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub async fn new(
        sink_id: String,
        sink_conf: SinkConf,
        kafka_client: Option<&Client>,
        app_err_tx: UnboundedSender<Option<Arc<String>>>,
    ) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = unbounded_channel();
        let (sink_err1, sink_err2) = BiLock::new(None);

        let partition_client = new_partition_client(kafka_client, &sink_conf).await;
        let task_loop = TaskLoop::new(
            sink_id,
            sink_conf,
            sink_err1,
            partition_client,
            app_err_tx,
            stop_signal_rx,
            mb_rx,
        );
        let join_handle = task_loop.start();

        Self {
            err: sink_err2,
            stop_signal_tx,
            mb_tx,
            join_handle: Some(join_handle),
        }
    }

    pub async fn update_conf(&mut self, _old_conf: SinkConf, sink_conf: SinkConf) {
        let mut task_loop = self.stop().await;
        task_loop.sink_conf = sink_conf;
        let join_handle = task_loop.start();
        self.join_handle = Some(join_handle);
    }

    pub async fn update_kafka_client(&mut self, kafka_client: Option<&Client>) {
        let mut task_loop = self.stop().await;
        let partition_client = new_partition_client(kafka_client, &task_loop.sink_conf).await;
        task_loop.partition_client = partition_client;
        let join_handle = task_loop.start();
        self.join_handle = Some(join_handle);
    }

    pub async fn stop(&mut self) -> TaskLoop {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }

    pub fn get_txs(&self, cnt: usize) -> Vec<UnboundedSender<RuleMessageBatch>> {
        let mut txs = vec![];
        for _ in 0..cnt {
            txs.push(self.mb_tx.clone());
        }
        txs
    }
}

fn transfer_unknown_topic_handling(
    unknown_topic_handling: &types::apps::kafka::UnknownTopicHandling,
) -> UnknownTopicHandling {
    match unknown_topic_handling {
        types::apps::kafka::UnknownTopicHandling::Error => UnknownTopicHandling::Error,
        types::apps::kafka::UnknownTopicHandling::Retry => UnknownTopicHandling::Retry,
    }
}

fn transfer_compression(compression: &types::apps::kafka::Compression) -> Compression {
    match compression {
        types::apps::kafka::Compression::None => Compression::NoCompression,
        types::apps::kafka::Compression::Gzip => Compression::Gzip,
        types::apps::kafka::Compression::Lz4 => Compression::Lz4,
        types::apps::kafka::Compression::Snappy => Compression::Snappy,
        types::apps::kafka::Compression::Zstd => Compression::Zstd,
    }
}

async fn new_partition_client(
    kafka_client: Option<&Client>,
    conf: &SinkConf,
) -> Option<PartitionClient> {
    match kafka_client {
        Some(kafka_client) => Some(
            kafka_client
                .partition_client(
                    &conf.topic,
                    conf.partition,
                    transfer_unknown_topic_handling(&conf.unknown_topic_handling),
                )
                .await
                .unwrap(),
        ),
        None => None,
    }
}
