use std::{
    fs::{File, OpenOptions},
    io::Write,
};

use anyhow::Result;
use message::MessageBatch;
use tokio::{select, sync::mpsc};
use tracing::debug;
use uuid::Uuid;

pub struct Logger {
    mb_tx: mpsc::Sender<MessageBatch>,
}

impl Logger {
    // 新建即启动
    pub async fn new(rule_id: &Uuid) -> Result<Self> {
        debug!("new logger");
        let (mb_tx, mb_rx) = mpsc::channel(16);

        Self::handle_message(rule_id, mb_rx).await?;

        Ok(Logger { mb_tx })
    }

    pub async fn handle_message(
        rule_id: &Uuid,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
    ) -> Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(rule_id.to_string())?;
        tokio::spawn(async move {
            loop {
                select! {
                    mb = mb_rx.recv() => {
                        debug!("get msg {:?}", mb);
                    }
                }
            }
        });

        Ok(())
    }

    fn log(file: &mut File, name: &str, message: &str) -> Result<()> {
        file.write_all(format!("{} {}", name, message).as_bytes())?;

        Ok(())
    }

    pub fn get_mb_tx(&self) -> mpsc::Sender<MessageBatch> {
        self.mb_tx.clone()
    }
}
