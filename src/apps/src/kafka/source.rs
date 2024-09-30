use std::sync::Arc;

use anyhow::{bail, Result};
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
use tracing::warn;
use types::apps::kafka::SourceConf;

use crate::kafka::{transfer_start_offset, transfer_unknown_topic_handling};

pub struct Source {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<
        JoinHandle<(
            SourceConf,
            watch::Receiver<()>,
            mpsc::Receiver<MessageBatch>,
        )>,
    >,
    pub mb_tx: broadcast::Sender<MessageBatch>,
}

impl Source {
    pub fn validate_conf(_conf: &SourceConf) -> HaliaResult<()> {
        Ok(())
    }

    pub async fn new(client: &Client, conf: SourceConf) -> HaliaResult<Self> {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, _) = broadcast::channel(16);

        let join_handle = Self::event_loop(client, conf, stop_signal_rx, mb_tx.clone()).await?;

        Ok(Self {
            stop_signal_tx,
            mb_tx,
            join_handle: Some(join_handle),
        })
    }

    async fn event_loop(
        client: &Client,
        conf: SourceConf,
        mut stop_signal_rx: watch::Receiver<()>,
        mb_tx: broadcast::Sender<MessageBatch>,
    ) -> Result<
        JoinHandle<(
            SourceConf,
            watch::Receiver<()>,
            mpsc::Receiver<MessageBatch>,
        )>,
    > {
        let partition_client = Arc::new(
            client
                .partition_client(
                    &conf.topic,
                    conf.partition,
                    transfer_unknown_topic_handling(&conf.unknown_topic_handling),
                )
                .await?,
        );

        let mut stream_consumer =
            StreamConsumerBuilder::new(partition_client, transfer_start_offset(conf.start_offset))
                .with_max_wait_ms(conf.max_wait)
                .build();

        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.changed() => {
                        return;
                    }

                    Some(res) = stream_consumer.next() => {
                        match res {
                            Ok(res) => Self::handle_kafka_msg(res, &mb_tx).await,
                            Err(_) => todo!(),
                        }
                        // Self::handle_kafka_msg(res).await?;
                    }
                }
            }
        });

        bail!("unreachable");
    }

    async fn handle_kafka_msg(
        res: (RecordAndOffset, i64),
        mb_tx: &broadcast::Sender<MessageBatch>,
    ) {
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

    pub async fn update_conf(
        &mut self,
        old_conf: SourceConf,
        new_conf: SourceConf,
    ) -> HaliaResult<()> {
        Ok(())
    }

    pub async fn stop(&mut self) {
        self.stop_signal_tx.send(()).unwrap();
        if let Some(join_handle) = self.join_handle.take() {
            join_handle.await.unwrap();
        }
    }
}
