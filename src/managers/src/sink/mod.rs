use anyhow::Result;
use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use sinks::{log::Log, Sink};
use std::{collections::HashMap, sync::LazyLock};
use tokio::sync::{broadcast::Receiver, RwLock};
use types::sink::{CreateSinkReq, ListSinkResp, ReadSinkResp, UpdateSinkReq};
use uuid::Uuid;

pub struct SinkManager {
    sinks: RwLock<HashMap<Uuid, Box<dyn Sink>>>,
}

pub static GLOBAL_SINK_MANAGER: LazyLock<SinkManager> = LazyLock::new(|| SinkManager {
    sinks: RwLock::new(HashMap::new()),
});

impl SinkManager {
    pub async fn create(&self, id: Option<Uuid>, req: CreateSinkReq) -> HaliaResult<()> {
        let id = match id {
            Some(id) => id,
            None => Uuid::new_v4(),
        };

        match req.r#type.as_str() {
            "log" => {
                let log = Log::new(req).unwrap();
                self.sinks.write().await.insert(id, Box::new(log));
            }
            _ => return Err(HaliaError::ProtocolNotSupported),
        }

        Ok(())
    }

    pub async fn read(&self, id: Uuid) -> HaliaResult<ReadSinkResp> {
        match self.sinks.read().await.get(&id) {
            Some(sink) => match sink.get_detail() {
                Ok(detail) => Ok(detail),
                Err(_) => todo!(),
            },
            None => return Err(HaliaError::NotFound),
        }
    }

    pub async fn list(&self) -> HaliaResult<Vec<ListSinkResp>> {
        Ok(self
            .sinks
            .read()
            .await
            .iter()
            .map(|(_, sink)| sink.get_info().unwrap())
            .collect())
    }

    pub async fn update(&self, id: Uuid, req: UpdateSinkReq) -> HaliaResult<()> {
        todo!()
    }

    pub async fn delete(&self, id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn insert_rx(&self, id: Uuid, rx: Receiver<MessageBatch>) -> Result<()> {
        // debug!("publish sink: {}", name);
        //     let sink = self.sinks.get_mut(name).unwrap();
        //     sink.insert_receiver(rx)
        todo!()
    }
}
