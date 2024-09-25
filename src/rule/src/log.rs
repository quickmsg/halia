use std::{
    fs::{File, OpenOptions},
    io::Write,
};

use anyhow::Result;
use message::MessageBatch;
use tokio::{
    select,
    sync::{broadcast, mpsc},
};
use tracing::{debug, info, warn};

pub struct Logger {
    mb_tx: mpsc::Sender<MessageBatch>,
}

impl Logger {
    // 新建即启动
    pub async fn new(rule_id: &String, stop_signal_rx: broadcast::Receiver<()>) -> Result<Self> {
        let (mb_tx, mb_rx) = mpsc::channel(16);

        Self::handle_message(rule_id, mb_rx, stop_signal_rx).await?;

        Ok(Logger { mb_tx })
    }

    pub async fn handle_message(
        rule_id: &String,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
        mut stop_signal_rx: broadcast::Receiver<()>,
    ) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!("logs/{}", rule_id.to_string()))?;
        tokio::spawn(async move {
            loop {
                select! {
                    mb = mb_rx.recv() => {
                        match mb {
                            Some(mb) => Self::log(&mut file, mb),
                            None => {}
                        }
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
        info!("{:?}", &mb);
        if let Err(e) = file.write_all(format!("{:?}\n", &mb.to_json()).as_bytes()) {
            warn!("write log to file err {}", e);
        }
        if let Err(e) = file.flush() {
            warn!("flush log err {}", e);
        }
    }

    pub fn get_mb_tx(&self) -> mpsc::Sender<MessageBatch> {
        self.mb_tx.clone()
    }
}
