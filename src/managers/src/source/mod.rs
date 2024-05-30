use anyhow::{bail, Result};
// use lazy_static::lazy_static;
use message::MessageBatch;
use sources::mqtt::Mqtt;
use sources::Source;
use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::Mutex;
use tokio::sync::broadcast::Receiver;
use tracing::{debug, error};
use types::rule::CreateSource;

pub struct Manager {
    pub sources: HashMap<String, Box<dyn Source + Sync>>,
}

pub static SOURCE_MANAGER: LazyLock<Mutex<Manager>> = LazyLock::new(|| {
    Mutex::new(Manager {
        sources: HashMap::new(),
    })
});

// pub fn register(create_source: CreateSource) -> Result<()> {
//     SOURCE_MANAGER.lock().unwrap().register(create_source)
// }

// pub fn get_receiver(name: String, graph_name: String) -> Result<Receiver<MessageBatch>> {
//     SOURCE_MANAGER
//         .lock()
//         .unwrap()
//         .get_receiver(&name, graph_name)
// }

impl Manager {
    pub fn register(&mut self, create_source: CreateSource) -> Result<()> {
        if self.sources.contains_key(&create_source.name) {
            bail!("已存在");
        }

        match create_source.r#type.as_str() {
            "mqtt" => match Mqtt::new(create_source.conf.clone()) {
                Ok(mqtt) => {
                    debug!("insert source");
                    self.sources.insert(create_source.name.clone(), mqtt);
                    return Ok(());
                }
                Err(e) => {
                    error!("register souce:{} err:{}", create_source.name, e);
                    bail!("todo")
                }
            },
            _ => bail!("not support"),
        }

        Ok(())
    }

    pub fn get_receiver(
        &mut self,
        name: &String,
        graph_name: String,
    ) -> Result<Receiver<MessageBatch>> {
        debug!("subscribe source: {}", name);
        match self.sources.get_mut(name) {
            Some(source) => source.subscribe(),
            None => {
                error!("don't have source:{}", name);
                bail!("")
            }
        }
    }

    fn stop() {
        // todo!()
    }
}
