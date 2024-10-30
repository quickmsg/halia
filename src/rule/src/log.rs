use std::{
    fs::{File, OpenOptions},
    io::Write,
};

use anyhow::Result;
use async_trait::async_trait;
use functions::Function;
use message::{MessageBatch, MessageValue, RuleMessageBatch};
use tokio::{
    select,
    sync::{
        broadcast,
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    },
};
use tracing::{debug, warn};

pub(crate) struct LogFunction {
    name: String,
}

impl LogFunction {
    pub fn new(name: String) -> Box<dyn Function> {
        Box::new(LogFunction { name })
    }
}

#[async_trait]
impl Function for LogFunction {
    async fn call(&self, message_batch: &mut MessageBatch) -> bool {
        message_batch.add_metadata(
            "log_name".to_owned(),
            MessageValue::String(self.name.clone()),
        );
        true
    }
}

pub struct Logger {
    tx: UnboundedSender<RuleMessageBatch>,
}

impl Logger {
    // 新建即启动
    pub async fn new(rule_id: &String, stop_signal_rx: broadcast::Receiver<()>) -> Result<Self> {
        let (tx, rx) = unbounded_channel();
        Self::handle_message(rule_id, rx, stop_signal_rx).await?;

        Ok(Logger { tx })
    }

    pub async fn handle_message(
        rule_id: &String,
        mut rx: UnboundedReceiver<RuleMessageBatch>,
        mut stop_signal_rx: broadcast::Receiver<()>,
    ) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!("logs/{}", rule_id.to_string()))?;
        tokio::spawn(async move {
            loop {
                select! {
                    Some(data) = rx.recv() => {
                        Self::log(&mut file, data);
                    }

                    _ = stop_signal_rx.recv() => {
                        debug!("log quit.");
                        return
                    }
                }
            }
        });

        Ok(())
    }

    fn log(file: &mut File, data: RuleMessageBatch) {
        debug!("log data: {:?}", data.take_mb());
        // if let Err(e) = file.write_all(data.as_bytes()) {
        //     warn!("write log to file err {}", e);
        // }

        if let Err(e) = file.flush() {
            warn!("flush log err {}", e);
        }
    }

    pub fn get_tx(&self) -> UnboundedSender<RuleMessageBatch> {
        self.tx.clone()
    }

    // pub fn get_broadcast_rx(&self) -> broadcast::Receiver<String> {
    //     self.broadcast_tx.subscribe()
    // }
}
