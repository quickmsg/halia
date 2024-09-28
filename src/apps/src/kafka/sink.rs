use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};
use common::error::HaliaResult;
use message::MessageBatch;
use rskafka::{
    client::{
        partition::{Compression, UnknownTopicHandling},
        Client,
    },
    record::Record,
};
use tokio::{
    select,
    sync::{mpsc, watch},
    task::JoinHandle,
};
use types::apps::kafka::SinkConf;

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    pub mb_tx: mpsc::Sender<MessageBatch>,
}

impl Sink {
    pub async fn new(id: String, conf: SinkConf, client: Client) -> Self {
        let partition_client = client
            .partition_client(
                conf.topic.to_owned(),
                conf.partition,
                UnknownTopicHandling::Retry,
            )
            .await
            .unwrap();

        let record = Record {
            key: None,
            value: Some(b"hello kafka".to_vec()),
            headers: BTreeMap::from([("foo".to_owned(), b"bar".to_vec())]),
            timestamp: Utc.timestamp_millis(42),
        };
        let compression = match conf.compression {
            types::apps::kafka::Compression::None => Compression::NoCompression,
            types::apps::kafka::Compression::Gzip => Compression::Gzip,
            types::apps::kafka::Compression::Lz4 => Compression::Lz4,
            types::apps::kafka::Compression::Snappy => Compression::Snappy,
            types::apps::kafka::Compression::Zstd => Compression::Zstd,
        };
        partition_client
            .produce(vec![record], compression)
            .await
            .unwrap();

        Sink {
            stop_signal_tx: todo!(),
            mb_tx: todo!(),
        }
    }

    fn event_loop(
        mut stop_signal_rx: watch::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
    ) -> JoinHandle<(watch::Receiver<()>, mpsc::Receiver<MessageBatch>)> {
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.changed() => {
                        return (stop_signal_rx, mb_rx)
                    }
                    Some(mb) = mb_rx.recv() => {
                        todo!()
                    }
                }
            }
        })
    }

    pub fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }
}
