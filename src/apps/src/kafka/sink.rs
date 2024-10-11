use std::collections::BTreeMap;

use anyhow::Result;
use base64::{engine::general_purpose, Engine};
use chrono::{TimeZone, Utc};
use common::{
    error::HaliaResult,
    sink_message_retain::{self, SinkMessageRetain},
};
use message::MessageBatch;
use rskafka::{
    client::{
        partition::{Compression, PartitionClient, UnknownTopicHandling},
        Client,
    },
    record::Record,
};
use tokio::{
    select,
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tracing::warn;
use types::apps::kafka::SinkConf;

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<JoinHandleData>>,
    pub mb_tx: mpsc::Sender<MessageBatch>,
}

pub struct JoinHandleData {
    pub partition_client: Option<PartitionClient>,
    pub kafka_err_tx: mpsc::Sender<String>,
    pub conf: SinkConf,
    pub stop_signal_rx: watch::Receiver<()>,
    pub mb_rx: mpsc::Receiver<MessageBatch>,
    pub message_retainer: Box<dyn SinkMessageRetain>,
}

impl Sink {
    pub fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub async fn new(
        kafka_client: Option<&Client>,
        kafka_err_tx: mpsc::Sender<String>,
        conf: SinkConf,
    ) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = mpsc::channel(16);

        let message_retainer = sink_message_retain::new(&conf.message_retain);
        let partition_client = new_partition_client(kafka_client, &conf).await;
        let join_handle_data = JoinHandleData {
            partition_client,
            kafka_err_tx,
            conf,
            stop_signal_rx,
            mb_rx,
            message_retainer,
        };
        let join_handle = Self::event_loop(join_handle_data).await;

        Self {
            stop_signal_tx,
            mb_tx,
            join_handle: Some(join_handle),
        }
    }

    async fn event_loop(mut join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        let compression = transfer_compression(&join_handle_data.conf.compression);
        tokio::spawn(async move {
            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        return join_handle_data;
                    }

                    Some(mb) = join_handle_data.mb_rx.recv() => {
                        match &join_handle_data.partition_client {
                            Some(partition_client) => {
                                if let Err(e) = Self::send_msg_to_kafka(&join_handle_data.conf, partition_client, mb, compression).await {
                                    warn!("{}", e);
                                    join_handle_data.kafka_err_tx.send(e.to_string()).await.unwrap();
                                }
                            }
                            None => {
                                join_handle_data.message_retainer.push(mb);
                            }
                        }

                    }
                }
            }
        })
    }

    async fn send_msg_to_kafka(
        conf: &SinkConf,
        partition_client: &PartitionClient,
        mb: MessageBatch,
        compression: Compression,
    ) -> Result<()> {
        let key = match &conf.key {
            Some(key) => match key.typ {
                types::ValueType::String => Some(key.value.as_bytes().to_vec()),
                types::ValueType::Bytes => {
                    let bytes = general_purpose::STANDARD.decode(&key.value).unwrap();
                    Some(bytes)
                }
            },
            None => None,
        };
        let mut headers = BTreeMap::new();
        if let Some(conf_headers) = &conf.headers {
            for (k, v) in conf_headers.iter() {
                match v.typ {
                    types::ValueType::String => {
                        headers.insert(k.clone(), v.value.as_bytes().to_vec());
                    }
                    types::ValueType::Bytes => {
                        let bytes = general_purpose::STANDARD.decode(&v.value).unwrap();
                        headers.insert(k.clone(), bytes);
                    }
                }
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

    pub async fn update_conf(&mut self, _old_conf: SinkConf, new_conf: SinkConf) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.conf = new_conf;
        let join_handle = Self::event_loop(join_handle_data).await;
        self.join_handle = Some(join_handle);
    }

    pub async fn update_kafka_client(&mut self, kafka_client: Option<&Client>) {
        let mut join_handle_data = self.stop().await;
        let partition_client = new_partition_client(kafka_client, &join_handle_data.conf).await;
        join_handle_data.partition_client = partition_client;
        let join_handle = Self::event_loop(join_handle_data).await;
        self.join_handle = Some(join_handle);
    }

    pub async fn stop(&mut self) -> JoinHandleData {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
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