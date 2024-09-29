use std::{collections::BTreeMap, sync::Arc};

use anyhow::Result;
use chrono::{TimeZone, Utc};
use common::error::HaliaResult;
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
    sync::{mpsc, watch, RwLock},
    task::JoinHandle,
};
use tracing::warn;
use types::apps::kafka::SinkConf;

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<
        JoinHandle<(
            SinkConf,
            Arc<RwLock<Option<Arc<Client>>>>,
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
        conf: SinkConf,
        client: Arc<RwLock<Option<Arc<Client>>>>,
    ) -> HaliaResult<Self> {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = mpsc::channel(16);

        let join_handle = Self::event_loop(conf, client, stop_signal_rx, mb_rx).await?;

        Ok(Self {
            stop_signal_tx,
            mb_tx,
            join_handle: Some(join_handle),
        })
    }

    async fn event_loop(
        conf: SinkConf,
        client: Arc<RwLock<Option<Arc<Client>>>>,
        mut stop_signal_rx: watch::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
    ) -> Result<
        JoinHandle<(
            SinkConf,
            Arc<RwLock<Option<Arc<Client>>>>,
            watch::Receiver<()>,
            mpsc::Receiver<MessageBatch>,
        )>,
    > {
        let partition_client = client
            .read()
            .await
            .as_ref()
            .unwrap()
            .clone()
            .partition_client(
                conf.topic.to_owned(),
                conf.partition,
                UnknownTopicHandling::Retry,
            )
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
                        return (conf, client, stop_signal_rx, mb_rx);
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
        msg: MessageBatch,
        compression: Compression,
    ) -> Result<()> {
        let record = Record {
            key: None,
            value: Some(msg.to_json()),
            headers: BTreeMap::from([("foo".to_owned(), b"bar".to_vec())]),
            timestamp: Utc.timestamp_millis(42),
        };
        partition_client.produce(vec![record], compression).await?;

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
