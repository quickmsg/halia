use std::{
    fs::{File, OpenOptions},
    io::Write,
};

use anyhow::Result;
use message::MessageBatch;
use tokio::{select, sync::broadcast};
use tracing::{debug, warn};

pub struct Logger {
    pub mb_tx: broadcast::Sender<MessageBatch>,
}

impl Logger {
    // 新建即启动
    pub async fn new(rule_id: &String, stop_signal_rx: broadcast::Receiver<()>) -> Result<Self> {
        let (mb_tx, mb_rx) = broadcast::channel(16);

        Self::handle_message(rule_id, mb_rx, stop_signal_rx).await?;

        Ok(Logger { mb_tx })
    }

    pub async fn handle_message(
        rule_id: &String,
        mut mb_rx: broadcast::Receiver<MessageBatch>,
        mut stop_signal_rx: broadcast::Receiver<()>,
    ) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!("logs/{}", rule_id.to_string()))?;
        tokio::spawn(async move {
            loop {
                select! {
                    Ok(mb) = mb_rx.recv() => {
                        Self::log(&mut file, mb);
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

    fn log(file: &mut File, mb: MessageBatch) {
        if let Err(e) = file.write_all(format!("{:?}, \n", mb).as_bytes()) {
            warn!("write log to file err {}", e);
        }

        if let Err(e) = file.flush() {
            warn!("flush log err {}", e);
        }
    }

    pub fn get_mb_tx(&self) -> broadcast::Sender<MessageBatch> {
        self.mb_tx.clone()
    }
}
