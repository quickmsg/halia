use std::sync::Arc;

use chrono::Utc;
use common::get_dynamic_value_from_json;
use log::warn;
use message::RuleMessageBatch;
use taos::{AsyncQueryable, Taos};
use tokio::{
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch,
    },
    task::JoinHandle,
};
use types::apps::tdengine::{SinkConf, TDengineConf};

use super::new_tdengine_client;

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<JoinHandleData>>,
    pub mb_tx: UnboundedSender<RuleMessageBatch>,
}

pub struct JoinHandleData {
    conf: SinkConf,
    taos: Taos,
    stop_signal_rx: watch::Receiver<()>,
    mb_rx: UnboundedReceiver<RuleMessageBatch>,
}

impl Sink {
    pub async fn new(conf: SinkConf, tdengine_conf: Arc<TDengineConf>) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = unbounded_channel();
        let taos = new_tdengine_client(&tdengine_conf, &conf).await;
        let join_handle_data = JoinHandleData {
            conf,
            taos,
            stop_signal_rx,
            mb_rx,
        };

        let join_handle = Self::event_loop(join_handle_data);

        Self {
            stop_signal_tx,
            join_handle: Some(join_handle),
            mb_tx,
        }
    }

    fn event_loop(mut join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        tokio::spawn(async move {
            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        return join_handle_data;
                    }
                    Some(mb) = join_handle_data.mb_rx.recv() => {
                        Self::handle_message_batch(&join_handle_data.conf, &join_handle_data.taos, mb).await;
                    }
                }
            }
        })
    }

    // INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31);
    async fn handle_message_batch(conf: &SinkConf, taos: &Taos, rmb: RuleMessageBatch) {
        let mut mb = rmb.take_mb();
        let mut values = vec![];
        let msg = mb.take_one_message().unwrap();
        for value in conf.values.iter() {
            match get_dynamic_value_from_json(value) {
                common::DynamicValue::Const(value) => {
                    match value {
                        serde_json::Value::Bool(b) => values.push(b.to_string()),
                        serde_json::Value::Number(number) => {
                            values.push(number.to_string());
                        }
                        serde_json::Value::String(s) => values.push(s),
                        _ => {}
                    }
                    // values.push(value),
                }
                common::DynamicValue::Field(field) => match msg.get(&field) {
                    Some(v) => match v {
                        message::MessageValue::Boolean(b) => values.push(b.to_string()),
                        message::MessageValue::Int64(i) => values.push(i.to_string()),
                        message::MessageValue::Float64(f) => values.push(f.to_string()),
                        message::MessageValue::String(s) => {
                            values.push(format!("'${}'", s.clone()))
                        }
                        _ => {
                            warn!("field {} type not supported", field);
                        }
                    },
                    None => {
                        warn!("field {} not found in message", field);
                        continue;
                    }
                },
            }
        }
        let values = values
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<String>>()
            .join(",");
        let ts = Utc::now().timestamp_millis();
        let sql = format!("INSERT INTO {} VALUES ({}, {});", conf.table, ts, values);
        if let Err(e) = taos.exec(&sql).await {
            warn!("{}", e);
        }
    }

    pub async fn stop(&mut self) -> JoinHandleData {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }

    pub async fn update_conf(&mut self, _old_conf: SinkConf, new_conf: SinkConf) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.conf = new_conf;
        let join_handle = Self::event_loop(join_handle_data);
        self.join_handle = Some(join_handle);
    }

    pub async fn update_tdengine_conf(&mut self, tdengine_conf: Arc<TDengineConf>) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.taos = new_tdengine_client(&tdengine_conf, &join_handle_data.conf).await;
        let join_handle = Self::event_loop(join_handle_data);
        self.join_handle = Some(join_handle);
    }

    pub fn get_txs(&self, cnt: usize) -> Vec<UnboundedSender<RuleMessageBatch>> {
        let mut txs = vec![];
        for _ in 0..cnt {
            txs.push(self.mb_tx.clone());
        }
        txs
    }
}
