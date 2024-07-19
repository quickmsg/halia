use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageBatch;
use protocol::coap::client::UdpCoAPClient;
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio::{
    select,
    sync::{broadcast, mpsc, RwLock},
    task::JoinHandle,
    time,
};
use tracing::{debug, error};
use types::devices::coap::{
    CreateUpdateGroupAPIReq, CreateUpdateGroupReq, SearchGroupAPIsResp, SearchGroupsItemResp,
};
use uuid::Uuid;

use super::group_api::API;

pub struct Group {
    pub id: Uuid,
    pub conf: CreateUpdateGroupReq,
    pub tx: Option<broadcast::Sender<MessageBatch>>,
    pub ref_cnt: usize,

    pub apis: Arc<RwLock<Vec<API>>>,

    pub stop_signal_tx: Option<mpsc::Sender<()>>,

    handle: Option<JoinHandle<(mpsc::Receiver<()>, Arc<UdpCoAPClient>)>>,
}

impl Group {
    pub async fn new(
        device_id: &Uuid,
        group_id: Option<Uuid>,
        req: CreateUpdateGroupReq,
    ) -> HaliaResult<Self> {
        let (group_id, new) = match group_id {
            Some(group_id) => (group_id, false),
            None => (Uuid::new_v4(), true),
        };

        if req.interval == 0 {
            return Err(HaliaError::ConfErr);
        }

        if new {
            persistence::devices::coap::create_group(
                device_id,
                &group_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Group {
            id: group_id,
            conf: req,
            tx: None,
            ref_cnt: 0,
            stop_signal_tx: None,
            apis: Arc::new(RwLock::new(vec![])),
            handle: None,
        })
    }

    pub async fn recover(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        match persistence::devices::coap::read_group_apis(device_id, &self.id).await {
            Ok(datas) => {
                for data in datas {
                    if data.len() == 0 {
                        continue;
                    }
                    let items = data.split(persistence::DELIMITER).collect::<Vec<&str>>();
                    assert_eq!(items.len(), 2);
                    let api_id = Uuid::from_str(items[0]).unwrap();
                    let req: CreateUpdateGroupAPIReq = serde_json::from_str(items[1])?;
                    self.create_api(device_id, Some(api_id), req).await?;
                }
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn search(&self) -> SearchGroupsItemResp {
        SearchGroupsItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub fn start(&mut self, coap_client: Arc<UdpCoAPClient>) {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        self.event_loop(coap_client, stop_signal_rx);
    }

    fn event_loop(
        &mut self,
        coap_client: Arc<UdpCoAPClient>,
        mut stop_signal_rx: mpsc::Receiver<()>,
    ) {
        let mut interval = time::interval(Duration::from_millis(self.conf.interval));
        let apis = self.apis.clone();
        let handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, coap_client);
                    }

                    _ = interval.tick() => {
                        Group::fetch_apis(&coap_client, &apis).await;
                    }
                }
            }
        });
        self.handle = Some(handle);
    }

    async fn fetch_apis(coap_client: &Arc<UdpCoAPClient>, apis: &Arc<RwLock<Vec<API>>>) {
        for api in apis.read().await.iter() {
            match coap_client.send(api.request.clone()).await {
                Ok(resp) => {
                    debug!("{:?}", resp,)
                }
                Err(e) => error!("{}", e),
            }
        }
    }

    pub async fn stop(&mut self) {
        match self.stop_signal_tx.as_ref().unwrap().send(()).await {
            Ok(()) => debug!("send stop signal ok"),
            Err(e) => debug!("send stop signal err :{e:?}"),
        }
        self.stop_signal_tx = None;
    }

    pub async fn update(&mut self, device_id: &Uuid, req: CreateUpdateGroupReq) -> HaliaResult<()> {
        persistence::devices::modbus::update_group(
            device_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        let mut restart = false;
        if self.conf.interval != req.interval {
            restart = true;
        }
        self.conf = req;

        if restart && self.stop_signal_tx.is_some() {
            self.stop_signal_tx.as_ref().unwrap().send(()).await;
            let (stop_signal_rx, coap_client) = self.handle.take().unwrap().await.unwrap();
            self.event_loop(coap_client, stop_signal_rx);
        }

        Ok(())
    }

    pub async fn delete(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        match self.stop_signal_tx {
            Some(_) => self.stop().await,
            None => {}
        }
        persistence::devices::modbus::delete_group(device_id, &self.id).await?;

        Ok(())
    }

    pub fn subscribe(&mut self) -> broadcast::Receiver<MessageBatch> {
        self.ref_cnt += 1;
        match &self.tx {
            Some(tx) => tx.subscribe(),
            None => {
                let (tx, rx) = broadcast::channel::<MessageBatch>(16);
                self.tx = Some(tx);
                rx
            }
        }
    }

    pub fn unsubscribe(&mut self) {
        self.ref_cnt -= 1;
        if self.ref_cnt == 0 {
            self.tx = None;
        }
    }

    pub async fn create_api(
        &mut self,
        device_id: &Uuid,
        api_id: Option<Uuid>,
        req: CreateUpdateGroupAPIReq,
    ) -> HaliaResult<()> {
        match API::new(device_id, &self.id, api_id, req).await {
            Ok(api) => Ok(self.apis.write().await.push(api)),
            Err(e) => Err(e),
        }
    }

    pub async fn search_apis(&self, page: usize, size: usize) -> SearchGroupAPIsResp {
        let mut data = vec![];
        for api in self.apis.read().await.iter().rev().skip((page - 1) * size) {
            data.push(api.search());
            if data.len() == size {
                break;
            }
        }

        SearchGroupAPIsResp {
            total: self.apis.read().await.len(),
            data,
        }
    }

    pub async fn update_api(
        &self,
        device_id: &Uuid,
        api_id: Uuid,
        req: CreateUpdateGroupAPIReq,
    ) -> HaliaResult<()> {
        match self
            .apis
            .write()
            .await
            .iter_mut()
            .find(|api| api.id == api_id)
        {
            Some(api) => api.update(device_id, &self.id, req).await,
            None => return Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_apis(&self, device_id: &Uuid, api_ids: Vec<Uuid>) -> HaliaResult<()> {
        for api_id in &api_ids {
            if let Some(api) = self.apis.read().await.iter().find(|api| api.id == *api_id) {
                api.delete(device_id, &self.id).await?;
            }
        }
        self.apis
            .write()
            .await
            .retain(|api| !api_ids.contains(&api.id));
        Ok(())
    }
}
