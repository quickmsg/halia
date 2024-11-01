use std::sync::Arc;

use common::error::{HaliaError, HaliaResult};
use log::debug;
use message::RuleMessageBatch;
use schema::Decoder;
use taos::StreamExt;
use tokio::{
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch,
    },
    task::JoinHandle,
};
use tokio_tungstenite::connect_async;
use types::apps::websocket::{AppConf, SourceConf};

use crate::websocket::connect_websocket;

pub struct Source {
    pub conf: SourceConf,
    pub mb_txs: Vec<UnboundedSender<RuleMessageBatch>>,
    pub decoder: Box<dyn Decoder>,
}

pub struct JoinHandleData {
    pub app_conf: Arc<AppConf>,
    pub source_conf: SourceConf,
    pub stop_signal_rx: watch::Receiver<()>,
    pub mb_txs: Vec<UnboundedSender<RuleMessageBatch>>,
}

impl Source {
    pub async fn new(app_conf: Arc<AppConf>, source_conf: SourceConf) -> Self {
        let decoder = schema::new_decoder(&source_conf.decode_type, &source_conf.schema_id)
            .await
            .unwrap();
        Source {
            conf: source_conf,
            mb_txs: vec![],
            decoder,
        }
    }

    pub async fn process_conf(id: &String, conf: &SourceConf) -> HaliaResult<()> {
        match conf.decode_type {
            types::schema::DecodeType::CsvWithSchema
            | types::schema::DecodeType::AvroWithSchema
            | types::schema::DecodeType::Protobuf => match &conf.schema_id {
                Some(schema_id) => {
                    schema::reference(schema::ResourceType::Device, schema_id, id).await?
                }
                None => return Err(HaliaError::Common("请填写schema_id".to_owned())),
            },
            _ => {}
        }

        Ok(())
    }

    async fn event_loop(mut join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        tokio::spawn(async move {
            let request = connect_websocket(
                &join_handle_data.app_conf,
                &join_handle_data.source_conf.path,
                &join_handle_data.source_conf.headers,
            );
            let (mut stream, response) = connect_async(request).await.unwrap();
            loop {
                select! {
                    msg = stream.next() => {
                        debug!("msg: {:?}", msg);
                    }

                    _ = join_handle_data.stop_signal_rx.changed() => {
                        return
                    }
                }
            }
        });
        todo!()
    }

    pub async fn update_conf(&mut self, _old_conf: SourceConf, _new_conf: SourceConf) {
        todo!()
    }

    pub fn get_rx(&mut self) -> UnboundedReceiver<RuleMessageBatch> {
        let (tx, rx) = unbounded_channel();
        self.mb_txs.push(tx);
        rx
    }

    pub async fn stop(&mut self) {}

    pub async fn update_app_conf(&mut self, app_conf: Arc<AppConf>) {
        todo!()
        // self.conf.app_id = app_conf.id.clone();
        // self.conf.app_conf = app_conf;
    }
}
