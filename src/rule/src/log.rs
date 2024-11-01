use std::{
    fs::{File, OpenOptions},
    io::Write,
};

use anyhow::Result;
use chrono::Local;
use message::{MessageValue, RuleMessageBatch};
use tokio::{
    select,
    sync::{
        broadcast,
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    },
};
use tracing::{debug, warn};

pub(crate) fn run_log(
    name: String,
    log_tx: UnboundedSender<String>,
    mut stop_signal_rx: broadcast::Receiver<()>,
) -> UnboundedSender<RuleMessageBatch> {
    let (tx, mut rx) = unbounded_channel();
    tokio::spawn(async move {
        loop {
            select! {
                Some(rmb) = rx.recv() => {
                    log_send_rule_message_batch(&name, rmb, &log_tx);
                }

                _ = stop_signal_rx.recv() => {
                    debug!("log quit.");
                    return
                }
            }
        }
    });
    tx
}

fn log_send_rule_message_batch(name: &String, rmb: RuleMessageBatch, tx: &UnboundedSender<String>) {
    let mut mb = rmb.take_mb();
    mb.add_metadata("log_name".to_owned(), MessageValue::String(name.clone()));
    tx.send(format!("{:?}\n", mb)).unwrap();
}

pub struct Logger {
    tx: UnboundedSender<String>,

    web_tx: broadcast::Sender<String>,
}

impl Logger {
    // 新建即启动
    pub async fn new(rule_id: &String, stop_signal_rx: broadcast::Receiver<()>) -> Result<Self> {
        let (tx, rx) = unbounded_channel();
        let (web_tx, _) = broadcast::channel(16);

        Self::handle_message(rule_id, rx, web_tx.clone(), stop_signal_rx).await?;

        Ok(Logger { tx, web_tx })
    }

    pub async fn handle_message(
        rule_id: &String,
        mut rx: UnboundedReceiver<String>,
        web_tx: broadcast::Sender<String>,
        mut stop_signal_rx: broadcast::Receiver<()>,
    ) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!("logs/{}.log", rule_id.to_string()))?;
        tokio::spawn(async move {
            loop {
                select! {
                    Some(data) = rx.recv() => {
                        Self::log(&mut file, data, &web_tx);
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

    fn log(file: &mut File, data: String, web_tx: &broadcast::Sender<String>) {
        println!("{}:      {}", Local::now(), data);
        let log = format!("{}:      {}", Local::now(), data);
        if web_tx.receiver_count() > 0 {
            web_tx.send(log.clone()).unwrap();
        }

        file.write_all(log.as_bytes()).unwrap();

        if let Err(e) = file.flush() {
            warn!("flush log err {}", e);
        }
    }

    pub fn get_tx(&self) -> UnboundedSender<String> {
        self.tx.clone()
    }

    pub fn get_web_rx(&self) -> broadcast::Receiver<String> {
        self.web_tx.subscribe()
    }
}
