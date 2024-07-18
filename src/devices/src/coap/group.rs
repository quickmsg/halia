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
    time,
};
use tracing::{debug, error};
use types::devices::coap::{
    CreateUpdateGroupReq, CreateUpdateGroupResourceReq, SearchGroupResourcesResp,
    SearchGroupsItemResp,
};
use uuid::Uuid;

use super::resource::Resource;

#[derive(Debug)]
pub struct Group {
    pub id: Uuid,
    pub conf: CreateUpdateGroupReq,
    pub tx: Option<broadcast::Sender<MessageBatch>>,
    pub ref_cnt: usize,

    pub resources: Arc<RwLock<Vec<Resource>>>,

    pub stop_signal_tx: Option<mpsc::Sender<()>>,
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
            // bail!("group interval must > 0")
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
            resources: Arc::new(RwLock::new(vec![])),
        })
    }

    pub async fn recover(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        match persistence::devices::coap::read_group_resources(device_id, &self.id).await {
            Ok(datas) => {
                for data in datas {
                    if data.len() == 0 {
                        continue;
                    }
                    let items = data.split(persistence::DELIMITER).collect::<Vec<&str>>();
                    assert_eq!(items.len(), 2);
                    let resource_id = Uuid::from_str(items[0]).unwrap();
                    let req: CreateUpdateGroupResourceReq = serde_json::from_str(items[1])?;
                    self.create_resource(device_id, Some(resource_id), req)
                        .await?;
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

    pub fn start(&mut self, client: Arc<UdpCoAPClient>) {
        let (stop_signal_tx, mut stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let interval = self.conf.interval;
        let resources = self.resources.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));
            loop {
                select! {
                    biased;
                    _ = stop_signal_rx.recv() => {
                        debug!("group stop");
                        return
                    }


                _ = interval.tick() => {
                    for resource in resources.read().await.iter() {
                        match client.send(resource.request.clone()).await {
                            Ok(resp) => {
                                debug!("{:?}", resp,)
                            }
                            Err(e) => error!("{}", e),
                        }
                    }
                }
                }
            }
        });
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
        // restart

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

    pub async fn create_resource(
        &mut self,
        device_id: &Uuid,
        resource_id: Option<Uuid>,
        req: CreateUpdateGroupResourceReq,
    ) -> HaliaResult<()> {
        match Resource::new(device_id, &self.id, resource_id, req).await {
            Ok(resource) => Ok(self.resources.write().await.push(resource)),
            Err(e) => Err(e),
        }
    }

    pub async fn search_resources(&self, page: usize, size: usize) -> SearchGroupResourcesResp {
        let mut data = vec![];
        for resource in self
            .resources
            .read()
            .await
            .iter()
            .rev()
            .skip((page - 1) * size)
        {
            data.push(resource.search());
            if data.len() == size {
                break;
            }
        }

        SearchGroupResourcesResp {
            total: self.resources.read().await.len(),
            data,
        }
    }

    pub async fn update_resource(
        &self,
        device_id: &Uuid,
        resource_id: Uuid,
        req: CreateUpdateGroupResourceReq,
    ) -> HaliaResult<()> {
        match self
            .resources
            .write()
            .await
            .iter_mut()
            .find(|resource| resource.id == resource_id)
        {
            Some(resource) => resource.update(device_id, &self.id, req).await,
            None => return Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_resources(
        &self,
        device_id: &Uuid,
        resource_ids: Vec<Uuid>,
    ) -> HaliaResult<()> {
        for resource_id in &resource_ids {
            if let Some(resource) = self
                .resources
                .read()
                .await
                .iter()
                .find(|resource| resource.id == *resource_id)
            {
                resource.delete(device_id, &self.id).await?;
            }
        }
        self.resources
            .write()
            .await
            .retain(|resource| !resource_ids.contains(&resource.id));
        Ok(())
    }
}
