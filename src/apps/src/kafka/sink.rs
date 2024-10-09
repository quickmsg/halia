use std::collections::BTreeMap;

use anyhow::Result;
use base64::{engine::general_purpose, Engine};
use chrono::{TimeZone, Utc};
use common::error::HaliaResult;
use message::MessageBatch;
use rskafka::{
    client::{
        partition::{Compression, PartitionClient},
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

use super::{transfer_compression, transfer_unknown_topic_handling};

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<
        JoinHandle<(
            mpsc::Sender<String>,
            SinkConf,
            watch::Receiver<()>,
            mpsc::Receiver<MessageBatch>,
        )>,
    >,
    pub mb_tx: mpsc::Sender<MessageBatch>,
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

        let join_handle =
            Some(Self::event_loop(kafka_client, kafka_err_tx, conf, stop_signal_rx, mb_rx).await);

        Self {
            stop_signal_tx,
            mb_tx,
            join_handle,
        }
    }

    async fn event_loop(
        kafka_client: Option<&Client>,
        kafka_err_tx: mpsc::Sender<String>,
        conf: SinkConf,
        mut stop_signal_rx: watch::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
    ) -> JoinHandle<(
        mpsc::Sender<String>,
        SinkConf,
        watch::Receiver<()>,
        mpsc::Receiver<MessageBatch>,
    )> {
        let unknown_topic_handling = transfer_unknown_topic_handling(&conf.unknown_topic_handling);
        // let mut err = false;
        let partition_client = match kafka_client {
            Some(kafka_client) => Some(
                kafka_client
                    .partition_client(&conf.topic, conf.partition, unknown_topic_handling)
                    .await
                    .unwrap(),
            ),
            None => None,
        };

        let compression = transfer_compression(&conf.compression);
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.changed() => {
                        return (kafka_err_tx, conf, stop_signal_rx, mb_rx);
                    }
                    Some(mb) = mb_rx.recv() => {
                        match &partition_client {
                            Some(partition_client) => {
                                if let Err(e) = Self::send_msg_to_kafka(&conf, partition_client, mb, compression).await {
                                    warn!("{}", e);
                                    kafka_err_tx.send(e.to_string()).await.unwrap();
                                }
                            }
                            None => {}
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
        for (k, v) in &conf.headers {
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
        let record = Record {
            key,
            value: Some(mb.to_json()),
            headers,
            timestamp: Utc.timestamp_millis_opt(0).unwrap(),
        };
        partition_client.produce(vec![record], compression).await?;
        Ok(())
    }

    pub async fn update_conf(
        &mut self,
        kafka_client: Option<&Client>,
        _old_conf: SinkConf,
        new_conf: SinkConf,
    ) {
        let (kafka_err_tx, _, stop_signal_rx, mb_rx) = self.stop().await;
        let join_handle =
            Self::event_loop(kafka_client, kafka_err_tx, new_conf, stop_signal_rx, mb_rx).await;
        self.join_handle = Some(join_handle);
    }

    pub async fn update_kafka_client(&mut self, kafka_client: Option<&Client>) {
        let (kafka_err_tx, conf, stop_signal_rx, mb_rx) = self.stop().await;
        let join_handle =
            Self::event_loop(kafka_client, kafka_err_tx, conf, stop_signal_rx, mb_rx).await;
        self.join_handle = Some(join_handle);
    }

    pub async fn stop(
        &mut self,
    ) -> (
        mpsc::Sender<String>,
        SinkConf,
        watch::Receiver<()>,
        mpsc::Receiver<MessageBatch>,
    ) {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }
}