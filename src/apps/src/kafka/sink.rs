use std::time::Duration;

use anyhow::{bail, Result};
use common::error::HaliaResult;
use kafka::{client::KafkaClient, producer::Producer};
use message::MessageBatch;
use tokio::{
    select,
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tracing::{debug, warn};
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

    pub async fn new(
        client: KafkaClient,
        conf: SinkConf,
        // client: Arc<RwLock<Option<Arc<Client>>>>,
    ) -> HaliaResult<Self> {
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
        client: KafkaClient,
        conf: SinkConf,
        mut stop_signal_rx: watch::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
    ) -> Result<JoinHandle<(SinkConf, watch::Receiver<()>, mpsc::Receiver<MessageBatch>)>> {
        let required_acks = match conf.required_acks {
            types::apps::kafka::RequiredAcks::None => kafka::client::RequiredAcks::None,
            types::apps::kafka::RequiredAcks::One => kafka::client::RequiredAcks::One,
            types::apps::kafka::RequiredAcks::All => kafka::client::RequiredAcks::All,
        };
        let compression = match conf.compression {
            types::apps::kafka::Compression::None => kafka::client::Compression::NONE,
            types::apps::kafka::Compression::Gzip => kafka::client::Compression::GZIP,
            types::apps::kafka::Compression::Lz4 => todo!(),
            types::apps::kafka::Compression::Snappy => kafka::client::Compression::SNAPPY,
            types::apps::kafka::Compression::Zstd => todo!(),
        };
        let mut producer = Producer::from_client(client)
            .with_ack_timeout(Duration::from_millis(conf.ack_timeout))
            .with_required_acks(required_acks)
            .with_compression(compression)
            // .with_connection_idle_timeout(cfg.conn_idle_timeout)
            .create()?;
        // let partition_client = client
        //     .read()
        //     .await
        //     .as_ref()
        //     .unwrap()
        //     .clone()
        //     .partition_client(
        //         conf.topic.to_owned(),
        //         conf.partition,
        //         UnknownTopicHandling::Retry,
        //     )
        //     .await?;
        // let compression = match conf.compression {
        //     types::apps::kafka::Compression::None => Compression::NoCompression,
        //     types::apps::kafka::Compression::Gzip => Compression::Gzip,
        //     types::apps::kafka::Compression::Lz4 => Compression::Lz4,
        //     types::apps::kafka::Compression::Snappy => Compression::Snappy,
        //     types::apps::kafka::Compression::Zstd => Compression::Zstd,
        // };
        let join_handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.changed() => {
                        return (conf, stop_signal_rx, mb_rx);
                    }
                    Some(mb) = mb_rx.recv() => {
                        if let Err(e) = Self::send_msg_to_kafka(&producer, mb, &conf.topic).await {
                            warn!("{}", e);
                        }
                    }
                }
            }
        });

        Ok(join_handle)
    }

    async fn send_msg_to_kafka(
        producer: &Producer,
        msg: MessageBatch,
        topic: &str,
        // compression: Compression,
    ) -> Result<()> {
        // let record = FutureRecord::to(topic)
        //     // TODO
        //     .payload("test")
        //     // TODO
        //     .key("test_key")
        //     .headers(OwnedHeaders::new().insert(Header {
        //         key: "header_key",
        //         value: Some("header_value"),
        //     }));
        // // let record = Record {
        // //     key: None,
        // //     value: Some(msg.to_json()),
        // //     headers: BTreeMap::from([("foo".to_owned(), b"bar".to_vec())]),
        // //     timestamp: Utc.timestamp_millis(42),
        // // };
        // // partition_client.produce(vec![record], compression).await?;
        // match producer.send(record, Duration::from_secs(0)).await {
        //     Ok(xx) => {
        //         debug!("send msg to kafka: {:?}", xx);
        //     }
        //     Err(e) => {
        //         debug!("send msg to kafka error: {:?}", e);
        //     }
        // }

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
