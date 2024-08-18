use std::sync::LazyLock;

use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::MessageBatch;
use tokio::sync::broadcast;
use types::{
    devices::{
        opcua::{
            CreateUpdateGroupReq, CreateUpdateGroupVariableReq, CreateUpdateOpcuaReq,
            CreateUpdateSinkReq, CreateUpdateSubscriptionReq, SearchGroupVariablesResp,
            SearchGroupsResp, SearchSinksResp, SearchSubscriptionsResp,
        },
        DeviceType, SearchDevicesItemResp,
    },
    Pagination,
};
use uuid::Uuid;

use crate::GLOBAL_DEVICE_MANAGER;

use super::Opcua;

pub static GLOBAL_OPCUA_MANAGER: LazyLock<Manager> = LazyLock::new(|| Manager {
    devices: DashMap::new(),
});

pub struct Manager {
    devices: DashMap<Uuid, Opcua>,
}

macro_rules! device_not_find_err {
    ($device_id:expr) => {
        Err(HaliaError::NotFound("Opcua设备".to_owned(), $device_id))
    };
}

impl Manager {
    pub async fn create(
        &self,
        device_id: Option<Uuid>,
        req: CreateUpdateOpcuaReq,
    ) -> HaliaResult<()> {
        GLOBAL_DEVICE_MANAGER.check_duplicate_name(&device_id, &req.base.name)?;

        let device = Opcua::new(device_id, req).await?;
        GLOBAL_DEVICE_MANAGER
            .create(DeviceType::Opcua, device.id)
            .await;
        self.devices.insert(device.id.clone(), device);
        Ok(())
    }

    pub fn check_duplicate_name(&self, device_id: &Option<Uuid>, name: &str) -> HaliaResult<()> {
        if self
            .devices
            .iter()
            .any(|device| !device.check_duplicate_name(device_id, name))
        {
            return Err(HaliaError::NameExists);
        }

        Ok(())
    }

    pub async fn recover(&self, device_id: &Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(device_id) {
            Some(mut device) => device.recover().await,
            None => device_not_find_err!(device_id.clone()),
        }
    }

    pub fn search(&self, device_id: &Uuid) -> HaliaResult<SearchDevicesItemResp> {
        match self.devices.get(device_id) {
            Some(device) => Ok(device.search()),
            None => device_not_find_err!(device_id.clone()),
        }
    }

    pub async fn update(&self, device_id: Uuid, req: CreateUpdateOpcuaReq) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.update(req).await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn start(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.start().await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn stop(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.stop().await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn delete(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.delete().await?,
            None => return device_not_find_err!(device_id),
        };

        self.devices.remove(&device_id);
        GLOBAL_DEVICE_MANAGER.delete(&device_id).await;

        Ok(())
    }

    pub async fn create_group(
        &self,
        device_id: Uuid,
        group_id: Option<Uuid>,
        req: CreateUpdateGroupReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.create_group(group_id, req).await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn search_groups(
        &self,
        device_id: Uuid,
        pagination: Pagination,
    ) -> HaliaResult<SearchGroupsResp> {
        match self.devices.get(&device_id) {
            Some(device) => device.search_groups(pagination).await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn update_group(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        req: CreateUpdateGroupReq,
    ) -> HaliaResult<()> {
        match self.devices.get(&device_id) {
            Some(device) => device.update_group(group_id, req).await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn delete_group(&self, device_id: Uuid, group_id: Uuid) -> HaliaResult<()> {
        match self.devices.get(&device_id) {
            Some(device) => device.delete_group(group_id).await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn create_group_variable(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        variable_id: Option<Uuid>,
        req: CreateUpdateGroupVariableReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => {
                device
                    .create_group_variable(group_id, variable_id, req)
                    .await
            }
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn search_group_variables(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        pagination: Pagination,
    ) -> HaliaResult<SearchGroupVariablesResp> {
        match self.devices.get(&device_id) {
            Some(device) => device.read_group_variables(group_id, pagination).await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn update_group_variable(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        variable_id: Uuid,
        req: CreateUpdateGroupVariableReq,
    ) -> HaliaResult<()> {
        match self.devices.get(&device_id) {
            Some(device) => {
                device
                    .update_group_variable(group_id, variable_id, req)
                    .await
            }
            None => device_not_find_err!(device_id),
        }
    }

    // pub async fn write_point_value(
    //     &self,
    //     device_id: Uuid,
    //     point_id: Uuid,
    //     value: serde_json::Value,
    // ) -> HaliaResult<()> {
    //     match self.devices.get_mut(&device_id) {
    //         Some(device) => device.write_point_value(point_id, value).await,
    //         None => Err(HaliaError::NotFound),
    //     }
    // }

    pub async fn delete_group_variable(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        variable_id: Uuid,
    ) -> HaliaResult<()> {
        match self.devices.get(&device_id) {
            Some(device) => device.delete_group_variable(group_id, variable_id).await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn add_group_ref(
        &self,
        device_id: &Uuid,
        group_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self.devices.get(device_id) {
            Some(device) => device.add_group_ref(group_id, rule_id).await,
            None => device_not_find_err!(device_id.clone()),
        }
    }

    pub async fn get_group_mb_rx(
        &self,
        device_id: &Uuid,
        group_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self.devices.get(device_id) {
            Some(device) => device.get_group_mb_rx(group_id, rule_id).await,
            None => device_not_find_err!(device_id.clone()),
        }
    }

    pub async fn del_group_mb_rx(
        &self,
        device_id: &Uuid,
        group_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self.devices.get(device_id) {
            Some(device) => device.del_group_mb_rx(group_id, rule_id).await,
            None => device_not_find_err!(device_id.clone()),
        }
    }

    pub async fn del_group_ref(
        &self,
        device_id: &Uuid,
        group_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self.devices.get(device_id) {
            Some(device) => device.del_group_ref(group_id, rule_id).await,
            None => device_not_find_err!(device_id.clone()),
        }
    }

    pub async fn create_subscription(
        &self,
        device_id: Uuid,
        subscription_id: Option<Uuid>,
        req: CreateUpdateSubscriptionReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.create_subscription(subscription_id, req).await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn search_subscriptions(
        &self,
        device_id: Uuid,
        pagination: Pagination,
    ) -> HaliaResult<SearchSubscriptionsResp> {
        match self.devices.get(&device_id) {
            Some(device) => device.search_subscriptions(pagination).await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn update_subscription(
        &self,
        device_id: Uuid,
        subscription_id: Uuid,
        req: CreateUpdateSubscriptionReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.update_subscription(subscription_id, req).await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn delete_subscription(
        &self,
        device_id: Uuid,
        subscription_id: Uuid,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.delete_subscription(subscription_id).await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn create_sink(
        &self,
        device_id: Uuid,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.create_sink(sink_id, req).await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn search_sinks(
        &self,
        device_id: Uuid,
        pagination: Pagination,
    ) -> HaliaResult<SearchSinksResp> {
        match self.devices.get(&device_id) {
            Some(device) => Ok(device.search_sinks(pagination).await),
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn update_sink(
        &self,
        device_id: Uuid,
        sink_id: Uuid,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.update_sink(sink_id, req).await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn delete_sink(&self, device_id: Uuid, sink_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.delete_sink(sink_id).await,
            None => device_not_find_err!(device_id),
        }
    }
}
