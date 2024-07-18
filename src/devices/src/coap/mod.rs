use common::{
    error::{HaliaError, HaliaResult},
    persistence::{self, Status},
};
use protocol::coap::client::UdpCoAPClient;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use types::devices::{coap::CreateUpdateCoapReq, SearchDevicesItemResp};
use uuid::Uuid;

pub const TYPE: &str = "coap";
mod group;
mod resource;
mod sink;

pub struct Coap {
    id: Uuid,
    conf: CreateUpdateCoapReq,
    client: Arc<RwLock<Option<UdpCoAPClient>>>,

    stop_signal_tx: Option<mpsc::Sender<()>>,
}

impl Coap {
    pub async fn new(device_id: Option<Uuid>, req: CreateUpdateCoapReq) -> HaliaResult<Self> {
        let (device_id, new) = match device_id {
            Some(device_id) => (device_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::devices::coap::create(&device_id, serde_json::to_string(&req).unwrap())
                .await?;
        }

        Ok(Coap {
            id: device_id,
            conf: todo!(),
            client: todo!(),
            stop_signal_tx: None,
        })
    }

    async fn run(&self) {
        // let conf = self.conf.clone();
        // let on = self.on.clone();
        // let client =
        //     UdpCoAPClient::new_udp((conf.read().await.host.clone(), conf.read().await.port)).await;

        // match client {
        //     Ok(client) => *self.client.write().await = Some(client),
        //     Err(e) => {
        //         error!("create client err :{}", e);
        //     }
        // }
    }

    // async fn recover(&mut self, status: Status) -> HaliaResult<()> {
    //     let paths = persistence::device::read_coap_paths(&self.id).await?;
    //     for (id, data) in paths {
    //         let path = path::new(id, &data).await?;
    //         self.paths.write().await.push(path);
    //     }
    //     if status == Status::Runing {
    //         self.start().await;
    //     }
    //     Ok(())
    // }

    pub fn search(&self) -> SearchDevicesItemResp {
        SearchDevicesItemResp {
            id: self.id.clone(),
            r#type: TYPE,
            on: todo!(),
            err: todo!(),
            rtt: todo!(),
            conf: json!(&self.conf),
        }
    }

    pub async fn update(&mut self, req: CreateUpdateCoapReq) -> HaliaResult<()> {
        persistence::devices::update_device_conf(&self.id, serde_json::to_string(&req).unwrap())
            .await?;

        let mut restart = false;
        if self.conf.host != req.host || self.conf.port != req.port {
            restart = true;
        }

        self.conf = req;

        if restart && self.stop_signal_tx.is_some() {
            // restart
            todo!()
        }

        Ok(())
    }

    pub async fn start(&mut self) -> HaliaResult<()> {
        if self.stop_signal_tx.is_some() {
            return Ok(());
        }

        persistence::devices::update_device_status(&self.id, Status::Runing).await?;
        let (stop_signal_tx, mut stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let conf = self.conf.clone();

        tokio::spawn(async move {});

        Ok(())
    }

    pub async fn stop(&mut self) -> HaliaResult<()> {
        if self.stop_signal_tx.is_none() {
            return Ok(());
        }
        persistence::devices::update_device_status(&self.id, Status::Stopped).await?;

        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
        self.stop_signal_tx = None;

        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        if self.stop_signal_tx.is_some() {
            return Err(HaliaError::DeviceRunning);
        }

        persistence::devices::delete_device(&self.id).await?;

        Ok(())
    }
}
