use std::collections::BTreeMap;

use anyhow::Result;
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

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<(SinkConf, watch::Receiver<()>, mpsc::Receiver<MessageBatch>)>>,
    pub mb_tx: mpsc::Sender<MessageBatch>,
}

impl Sink {
    pub fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub async fn new(client: &Client, conf: SinkConf) -> HaliaResult<Self> {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = mpsc::channel(16);

        let join_handle = Self::event_loop(client, conf, stop_signal_rx, mb_rx).await?;

        Ok(Self {
            stop_signal_tx,
            mb_tx,
            join_handle: Some(join_handle),
        })
    }

    async fn event_loop(
        client: &Client,
        conf: SinkConf,
        mut stop_signal_rx: watch::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
    ) -> Result<JoinHandle<(SinkConf, watch::Receiver<()>, mpsc::Receiver<MessageBatch>)>> {
        let unknown_topic_handling = match conf.unknown_topic_handling {
            types::apps::kafka::UnknownTopicHandling::Error => {
                rskafka::client::partition::UnknownTopicHandling::Error
            }
            types::apps::kafka::UnknownTopicHandling::Retry => {
                rskafka::client::partition::UnknownTopicHandling::Retry
            }
        };
        let partition_client = client
            .partition_client(&conf.topic, conf.partition, unknown_topic_handling)
            .await?;

        let compression = match conf.compression {
            types::apps::kafka::Compression::None => Compression::NoCompression,
            types::apps::kafka::Compression::Gzip => Compression::Gzip,
            types::apps::kafka::Compression::Lz4 => Compression::Lz4,
            types::apps::kafka::Compression::Snappy => Compression::Snappy,
            types::apps::kafka::Compression::Zstd => Compression::Zstd,
        };
        let join_handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.changed() => {
                        return (conf, stop_signal_rx, mb_rx);
                    }
                    Some(mb) = mb_rx.recv() => {
                        if let Err(e) = Self::send_msg_to_kafka(&partition_client, mb, compression).await {
                            warn!("{}", e);
                        }
                    }
                }
            }
        });

        Ok(join_handle)
    }

    async fn send_msg_to_kafka(
        partition_client: &PartitionClient,
        mb: MessageBatch,
        compression: Compression,
    ) -> Result<()> {
        let record = Record {
            key: None,
            value: Some(mb.to_json()),
            headers: BTreeMap::from([("foo".to_owned(), b"bar".to_vec())]),
            timestamp: Utc.timestamp_millis(42),
        };
        partition_client
            .produce(vec![record], compression)
            .await
            .unwrap();

        Ok(())
    }

    pub async fn update_conf(&mut self, old_conf: SinkConf, new_conf: SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub async fn stop(&mut self) {
        self.stop_signal_tx.send(()).unwrap();
        if let Some(join_handle) = self.join_handle.take() {
            join_handle.await.unwrap();
        }
    }
}
