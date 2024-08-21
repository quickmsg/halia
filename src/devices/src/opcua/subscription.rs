use std::{sync::Arc, time::Duration};

use anyhow::Result;
use common::{
    del_mb_rx,
    error::{HaliaError, HaliaResult},
    get_id, get_mb_rx, persistence,
    ref_info::RefInfo,
};
use message::MessageBatch;
use opcua::{
    client::{DataChangeCallback, MonitoredItem, Session},
    types::{DataValue, MonitoredItemCreateRequest, NodeId, TimestampsToReturn},
};
use tokio::sync::broadcast;
use types::devices::opcua::{CreateUpdateSubscriptionReq, SearchSubscriptionsItemResp};
use uuid::Uuid;

pub struct Subscription {
    pub id: Uuid,
    conf: CreateUpdateSubscriptionReq,

    monitored_items: Vec<MonitoredItem>,

    on: bool,

    pub ref_info: RefInfo,
    mb_tx: Option<broadcast::Sender<MessageBatch>>,
}

impl Subscription {
    pub async fn new(
        device_id: &Uuid,
        subscription_id: Option<Uuid>,
        req: CreateUpdateSubscriptionReq,
    ) -> HaliaResult<Self> {
        Self::check_conf(&req)?;

        let (subscription_id, new) = get_id(subscription_id);
        if new {
            // persistence::devices::opcua::create_subscription(
            //     device_id,
            //     &subscription_id,
            //     serde_json::to_string(&req).unwrap(),
            // )
            // .await?;
        }

        Ok(Self {
            id: subscription_id,
            conf: req,
            on: false,
            ref_info: RefInfo::new(),
            mb_tx: None,
            monitored_items: vec![],
        })
    }

    fn check_conf(req: &CreateUpdateSubscriptionReq) -> HaliaResult<()> {
        if req.ext.publishing_interval == 0 {
            return Err(HaliaError::Common("发布间隔必须大于0!".to_owned()));
        }

        Ok(())
    }

    pub fn check_duplicate(&self, req: &CreateUpdateSubscriptionReq) -> HaliaResult<()> {
        if self.conf.base.name == req.base.name {
            return Err(HaliaError::NameExists);
        }

        if self.conf.ext.monitored_items == req.ext.monitored_items {
            return Err(HaliaError::Common("点位重复！".to_owned()));
        }

        Ok(())
    }

    pub fn search(&self) -> SearchSubscriptionsItemResp {
        SearchSubscriptionsItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
            rule_ref: self.ref_info.get_rule_ref(),
        }
    }

    pub async fn start(&mut self, session: Arc<Session>) -> Result<()> {
        let opcua_subscription_id = session
            .create_subscription(
                Duration::from_secs(self.conf.ext.publishing_interval),
                self.conf.ext.lifetime_count,
                self.conf.ext.max_keep_alive_count,
                self.conf.ext.max_notifications_per_publish,
                self.conf.ext.priority,
                self.conf.ext.publishing_enalbed,
                DataChangeCallback::new(|dv, item| {
                    println!("Data change from server:");
                    print_value(&dv, item);
                }),
            )
            .await?;

        // TODO
        let ns = 2;
        let items_to_create: Vec<MonitoredItemCreateRequest> = ["v1", "v2", "v3", "v4"]
            .iter()
            .map(|v| NodeId::new(ns, *v).into())
            .collect();

        let _ = session
            .create_monitored_items(
                opcua_subscription_id,
                TimestampsToReturn::Both,
                items_to_create,
            )
            .await?;

        Ok(())
    }

    pub async fn update(
        &mut self,
        device_id: &Uuid,
        req: CreateUpdateSubscriptionReq,
    ) -> HaliaResult<()> {
        todo!()
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        if !self.ref_info.can_delete() {
            return Err(HaliaError::DeleteRefing);
        }

        Ok(())
    }

    pub fn get_mb_rx(&mut self, rule_id: &Uuid) -> broadcast::Receiver<MessageBatch> {
        get_mb_rx!(self, rule_id)
    }

    pub fn del_mb_rx(&mut self, rule_id: &Uuid) {
        del_mb_rx!(self, rule_id);
    }
}

fn print_value(data_value: &DataValue, item: &MonitoredItem) {
    let node_id = &item.item_to_monitor().node_id;
    if let Some(ref value) = data_value.value {
        println!("Item \"{}\", Value = {:?}", node_id, value);
    } else {
        println!(
            "Item \"{}\", Value not found, error: {}",
            node_id,
            data_value.status.as_ref().unwrap()
        );
    }
}
