#![feature(lazy_cell)]

use anyhow::{bail, Result};
use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use log::Log;
use message::MessageBatch;
use std::{collections::HashMap, sync::LazyLock};
use tokio::sync::{broadcast::Receiver, RwLock};
use tracing::debug;
use types::sink::{CreateSinkReq, ListSinkResp, ReadSinkResp, UpdateSinkReq};
use uuid::Uuid;

pub mod log;

pub struct SinkManager {
    sinks: RwLock<HashMap<Uuid, Box<dyn Sink>>>,
}

pub static GLOBAL_SINK_MANAGER: LazyLock<SinkManager> = LazyLock::new(|| SinkManager {
    sinks: RwLock::new(HashMap::new()),
});

impl SinkManager {
    pub async fn create(&self, id: Option<Uuid>, req: CreateSinkReq) -> HaliaResult<()> {
        let (id, persistence) = match id {
            Some(id) => (id, false),
            None => (Uuid::new_v4(), true),
        };

        let sink = match req.r#type.as_str() {
            "log" => Log::new(id, &req)?,
            _ => return Err(HaliaError::ProtocolNotSupported),
        };

        self.sinks.write().await.insert(id, sink);

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

    pub async fn insert_rx(&self, id: &Uuid, rx: Receiver<MessageBatch>) -> Result<()> {
        debug!("publish sink: {}", id);
        match self.sinks.write().await.get_mut(&id) {
            Some(sink) => sink.insert_receiver(rx),
            None => bail!("not find"),
        }
    }
}

pub trait Sink: Send + Sync {
    fn get_detail(&self) -> Result<ReadSinkResp>;

    fn get_info(&self) -> Result<ListSinkResp>;

    fn insert_receiver(&mut self, receiver: Receiver<MessageBatch>) -> Result<()>;
}
