use anyhow::{anyhow, bail, Result};
use message::MessageBatch;
use sinks::{log::Log, Sink};
use std::{
    collections::HashMap,
    sync::{LazyLock, Mutex},
};
use tokio::sync::broadcast::Receiver;
use tracing::debug;
use types::rule::CreateSink;

pub struct Manager {
    sinks: HashMap<String, Box<dyn Sink + Sync>>,
}

pub static SINK_MANAGER: LazyLock<Mutex<Manager>> = LazyLock::new(|| {
    Mutex::new(Manager {
        sinks: HashMap::new(),
    })
});

impl Manager {
    pub fn register(&mut self, create_sink: CreateSink) -> Result<()> {
        if self.sinks.contains_key(&create_sink.name) {
            return Err(anyhow!("已存在"));
        }

        match create_sink.r#type.as_str() {
            "log" => {
                let log = Log::new().unwrap();
                self.sinks.insert(create_sink.name.clone(), Box::new(log));
            }
            _ => bail!("not supprt"),
        }

        Ok(())
    }

    pub fn insert_receiver(&mut self, name: &String, rx: Receiver<MessageBatch>) -> Result<()> {
        debug!("publish sink: {}", name);
        let sink = self.sinks.get_mut(name).unwrap();
        sink.insert_receiver(rx)
    }
}
