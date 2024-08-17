use anyhow::Result;
use std::{sync::Arc, time::Duration};

use common::{error::HaliaResult, get_id, persistence};
use opcua::{
    client::{DataChangeCallback, MonitoredItem, Session},
    types::{DataValue, MonitoredItemCreateRequest, NodeId, TimestampsToReturn},
};
use types::devices::opcua::{CreateUpdateSubscriptionReq, SearchSubscriptionsItemResp};
use uuid::Uuid;

pub struct Subscription {
    pub id: Uuid,
    conf: CreateUpdateSubscriptionReq,

    on: bool,
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
            persistence::devices::opcua::create_subscription(
                device_id,
                &subscription_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Self {
            id: subscription_id,
            conf: req,
            on: false,
        })
    }

    fn check_conf(req: &CreateUpdateSubscriptionReq) -> HaliaResult<()> {
        Ok(())
    }

    pub fn check_duplicate(&self, req: &CreateUpdateSubscriptionReq) -> HaliaResult<()> {
        Ok(())
    }

    pub fn search(&self) -> SearchSubscriptionsItemResp {
        SearchSubscriptionsItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub async fn start(&mut self, session: Arc<Session>) -> Result<()> {
        self.on = true;

        let subscription_id = session
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
            .create_monitored_items(subscription_id, TimestampsToReturn::Both, items_to_create)
            .await?;

        Ok(())
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
