#![feature(lazy_cell)]

use anyhow::{bail, Result};
use common::{
    error::{HaliaError, HaliaResult},
    persistence::{self, sink},
};
use log::Log;
use message::MessageBatch;
use std::{collections::HashMap, sync::LazyLock};
use tokio::sync::{broadcast::Receiver, RwLock};
use tracing::{debug, error};
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

        if persistence {
            if let Err(e) = sink::insert(id, serde_json::to_string(&req).unwrap()).await {
                error!("write sink err: {}", e);
            }
        }

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

impl SinkManager {
    pub async fn recover(&self) -> HaliaResult<()> {
        match persistence::sink::read().await {
            Ok(sources) => {
                for (id, data) in sources {
                    let req = match serde_json::from_str::<CreateSinkReq>(&data) {
                        Ok(req) => req,
                        Err(e) => {
                            error!("parse json file err:{}", e);
                            return Err(e.into());
                        }
                    };
                    GLOBAL_SINK_MANAGER.create(Some(id), req).await?;
                }
                Ok(())
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => match persistence::sink::init().await {
                    Ok(_) => Ok(()),
                    Err(e) => return Err(e.into()),
                },
                std::io::ErrorKind::PermissionDenied => todo!(),
                std::io::ErrorKind::ConnectionRefused => todo!(),
                std::io::ErrorKind::ConnectionReset => todo!(),
                std::io::ErrorKind::ConnectionAborted => todo!(),
                std::io::ErrorKind::NotConnected => todo!(),
                std::io::ErrorKind::AddrInUse => todo!(),
                std::io::ErrorKind::AddrNotAvailable => todo!(),
                std::io::ErrorKind::BrokenPipe => todo!(),
                std::io::ErrorKind::AlreadyExists => todo!(),
                std::io::ErrorKind::WouldBlock => todo!(),
                std::io::ErrorKind::InvalidInput => todo!(),
                std::io::ErrorKind::InvalidData => todo!(),
                std::io::ErrorKind::TimedOut => todo!(),
                std::io::ErrorKind::WriteZero => todo!(),
                std::io::ErrorKind::Interrupted => todo!(),
                std::io::ErrorKind::Unsupported => todo!(),
                std::io::ErrorKind::UnexpectedEof => todo!(),
                std::io::ErrorKind::OutOfMemory => todo!(),
                std::io::ErrorKind::Other => todo!(),
                _ => todo!(),
            },
        }
    }
}

pub trait Sink: Send + Sync {
    fn get_detail(&self) -> Result<ReadSinkResp>;

    fn get_info(&self) -> Result<ListSinkResp>;

    fn insert_receiver(&mut self, receiver: Receiver<MessageBatch>) -> Result<()>;
}
