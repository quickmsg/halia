use std::sync::Arc;

use bytes::Bytes;
use common::error::HaliaResult;
use futures::StreamExt as _;
use message::MessageBatch;
use rskafka::{
    client::{consumer::StreamConsumerBuilder, Client},
    record::RecordAndOffset,
};
use tokio::{
    select,
    sync::{broadcast, mpsc, watch},
    task::JoinHandle,
};
use tracing::{debug, warn};
use types::apps::kafka::SourceConf;

use crate::kafka::{transfer_start_offset, transfer_unknown_topic_handling};

pub struct Source {
    stop_signal_tx: watch::Sender<()>,
    conf: Option<SourceConf>,
    kafka_err_tx: Option<mpsc::Sender<String>>,
    join_handle: Option<JoinHandle<(SourceConf, watch::Receiver<()>, mpsc::Sender<String>)>>,
    pub mb_tx: broadcast::Sender<MessageBatch>,
}

impl Source {
    pub fn validate_conf(_conf: &SourceConf) -> HaliaResult<()> {
        Ok(())
    }

    pub async fn new(
        kafka_client: Option<&Client>,
        kafka_err_tx: mpsc::Sender<String>,
        conf: SourceConf,
    ) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, _) = broadcast::channel(16);

        let (join_handle, conf, kafka_err_tx) = match kafka_client {
            Some(kafka_client) => (
                Some(
                    Self::event_loop(
                        kafka_client,
                        kafka_err_tx,
                        conf,
                        stop_signal_rx,
                        mb_tx.clone(),
                    )
                    .await,
                ),
                None,
                None,
            ),
            None => (None, Some(conf), Some(kafka_err_tx)),
        };

        Self {
            stop_signal_tx,
            mb_tx,
            conf,
            join_handle,
            kafka_err_tx,
        }
    }

    async fn event_loop(
        client: &Client,
        kafka_err_tx: mpsc::Sender<String>,
        conf: SourceConf,
        mut stop_signal_rx: watch::Receiver<()>,
        mb_tx: broadcast::Sender<MessageBatch>,
    ) -> JoinHandle<(SourceConf, watch::Receiver<()>, mpsc::Sender<String>)> {
        let partition_client = Arc::new(
            client
                .partition_client(
                    &conf.topic,
                    conf.partition,
                    transfer_unknown_topic_handling(&conf.unknown_topic_handling),
                )
                .await
                .unwrap(),
        );

        let mut stream_consumer =
            StreamConsumerBuilder::new(partition_client, transfer_start_offset(&conf.start_offset))
                .with_max_wait_ms(conf.max_wait)
                .build();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.changed() => {
                        return (conf, stop_signal_rx, kafka_err_tx);
                    }

                    Some(res) = stream_consumer.next() => {
                        match res {
                            Ok(res) => Self::handle_kafka_msg(res, &mb_tx).await,
                            Err(e) => kafka_err_tx.send(e.to_string()).await.unwrap(),
                        }
                    }
                }
            }
        })
    }

    async fn handle_kafka_msg(
        res: (RecordAndOffset, i64),
        mb_tx: &broadcast::Sender<MessageBatch>,
    ) {
        debug!("kafka message: {:?}", res);
        if mb_tx.receiver_count() == 0 {
            return;
        }
        match res.0.record.value {
            Some(data) => match MessageBatch::from_json(Bytes::from(data)) {
                Ok(mb) => {
                    mb_tx.send(mb).unwrap();
                }
                Err(e) => warn!("parse kafka message error: {}", e),
            },
            None => {}
        }
    }

    pub async fn update_kafka_client(&mut self, kafka_client: Option<&Client>) {
        match self.join_handle.is_some() {
            true => {
                let (conf, stop_signal_rx, kafka_err_tx) = self.stop().await;
                match kafka_client {
                    Some(kafka_client) => {
                        let join_handle = Self::event_loop(
                            kafka_client,
                            kafka_err_tx,
                            conf,
                            stop_signal_rx,
                            self.mb_tx.clone(),
                        )
                        .await;
                        self.join_handle = Some(join_handle);
                    }
                    None => todo!(),
                }
            }
            false => match kafka_client {
                Some(kafka_client) => {
                    let conf = self.conf.take().unwrap();
                    let stop_signal_rx = self.stop_signal_tx.subscribe();
                    let kafka_err_tx = self.kafka_err_tx.take().unwrap();
                    let join_handle = Self::event_loop(
                        kafka_client,
                        kafka_err_tx,
                        conf,
                        stop_signal_rx,
                        self.mb_tx.clone(),
                    )
                    .await;
                    self.join_handle = Some(join_handle);
                }
                None => {}
            },
        }
    }

    pub async fn update_conf(
        &mut self,
        kafka_client: Option<&Client>,
        _old_conf: SourceConf,
        new_conf: SourceConf,
    ) {
        match self.join_handle.is_some() {
            true => {
                let (_, stop_signal_rx, kafka_err_tx) = self.stop().await;
                match kafka_client {
                    Some(kafka_client) => {
                        let join_handle = Self::event_loop(
                            kafka_client,
                            kafka_err_tx,
                            new_conf,
                            stop_signal_rx,
                            self.mb_tx.clone(),
                        )
                        .await;
                        self.join_handle = Some(join_handle);
                    }
                    None => todo!(),
                }
            }
            false => match kafka_client {
                Some(kafka_client) => {
                    let stop_signal_rx = self.stop_signal_tx.subscribe();
                    let kafka_err_tx = self.kafka_err_tx.take().unwrap();
                    let join_handle = Self::event_loop(
                        kafka_client,
                        kafka_err_tx,
                        new_conf,
                        stop_signal_rx,
                        self.mb_tx.clone(),
                    )
                    .await;
                    self.join_handle = Some(join_handle);
                }
                None => {}
            },
        }
    }

    pub async fn stop(&mut self) -> (SourceConf, watch::Receiver<()>, mpsc::Sender<String>) {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }
}
