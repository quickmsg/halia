use std::{
    fs::{File, OpenOptions},
    io::Write,
};

use anyhow::Result;
use tokio::{
    select,
    sync::{
        broadcast,
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    },
};
use tracing::{debug, warn};

pub struct Logger {
    tx: UnboundedSender<String>,
    broadcast_tx: broadcast::Sender<String>,
}

impl Logger {
    // 新建即启动
    pub async fn new(rule_id: &String, stop_signal_rx: broadcast::Receiver<()>) -> Result<Self> {
        let (tx, rx) = unbounded_channel();
        let (broadcast_tx, _) = broadcast::channel(16);
        Self::handle_message(rule_id, rx, stop_signal_rx, broadcast_tx.clone()).await?;

        Ok(Logger { tx, broadcast_tx })
    }

    pub async fn handle_message(
        rule_id: &String,
        mut rx: UnboundedReceiver<String>,
        mut stop_signal_rx: broadcast::Receiver<()>,
        broadcast_tx: broadcast::Sender<String>,
    ) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!("logs/{}", rule_id.to_string()))?;
        tokio::spawn(async move {
            loop {
                select! {
                    Some(data) = rx.recv() => {
                        Self::log(&mut file, data, &broadcast_tx);
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

    fn log(file: &mut File, data: String, broadcast_tx: &broadcast::Sender<String>) {
        if broadcast_tx.receiver_count() > 0 {
            broadcast_tx.send(data.clone()).unwrap();
        }
        if let Err(e) = file.write_all(data.as_bytes()) {
            warn!("write log to file err {}", e);
        }

        if let Err(e) = file.flush() {
            warn!("flush log err {}", e);
        }
    }

    pub fn get_tx(&self) -> UnboundedSender<String> {
        self.tx.clone()
    }

    pub fn get_broadcast_rx(&self) -> broadcast::Receiver<String> {
        self.broadcast_tx.subscribe()
    }
}
