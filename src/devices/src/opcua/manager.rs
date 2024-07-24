use std::sync::LazyLock;

use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::MessageBatch;
use tokio::sync::broadcast;
use types::devices::{
    opcua::{
        CreateUpdateGroupReq, CreateUpdateGroupVariableReq, CreateUpdateOpcuaReq,
        SearchGroupVariablesResp, SearchGroupsResp,
    },
    SearchDevicesItemResp,
};
use uuid::Uuid;

use crate::GLOBAL_DEVICE_MANAGER;

use super::{Opcua, TYPE};

pub static GLOBAL_OPCUA_MANAGER: LazyLock<Manager> = LazyLock::new(|| Manager {
    devices: DashMap::new(),
});

pub struct Manager {
    devices: DashMap<Uuid, Opcua>,
}

impl Manager {
    pub async fn create(
        &self,
        device_id: Option<Uuid>,
        req: CreateUpdateOpcuaReq,
    ) -> HaliaResult<()> {
        let device = Opcua::new(device_id, req).await?;
        GLOBAL_DEVICE_MANAGER.create(&TYPE, device.id).await;
        self.devices.insert(device.id.clone(), device);
        Ok(())
    }

    pub async fn recover(&self, device_id: &Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(device_id) {
            Some(mut device) => device.recover().await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub fn search(&self, device_id: &Uuid) -> HaliaResult<SearchDevicesItemResp> {
        match self.devices.get(device_id) {
            Some(device) => Ok(device.search()),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update(&self, device_id: Uuid, req: CreateUpdateOpcuaReq) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.update(req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn start(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.start().await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn stop(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.stop().await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => {
                device.delete().await?;
            }
            None => return Err(HaliaError::NotFound),
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
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn search_groups(
        &self,
        device_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchGroupsResp> {
        match self.devices.get(&device_id) {
            Some(device) => device.search_groups(page, size).await,
            None => Err(HaliaError::NotFound),
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
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_group(&self, device_id: Uuid, group_id: Uuid) -> HaliaResult<()> {
        match self.devices.get(&device_id) {
            Some(device) => device.delete_group(group_id).await,
            None => Err(HaliaError::NotFound),
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
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn search_group_variables(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchGroupVariablesResp> {
        match self.devices.get(&device_id) {
            Some(device) => device.read_group_variables(group_id, page, size).await,
            None => Err(HaliaError::NotFound),
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
            None => Err(HaliaError::NotFound),
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
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn add_subscribe_ref(
        &self,
        device_id: &Uuid,
        group_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self.devices.get(device_id) {
            Some(device) => device.add_subscribe_ref(group_id, rule_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn subscribe(
        &self,
        device_id: &Uuid,
        group_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self.devices.get(device_id) {
            Some(device) => device.subscribe(group_id, rule_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn unsubscribe(
        &self,
        device_id: &Uuid,
        group_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self.devices.get(device_id) {
            Some(device) => device.unsubscribe(group_id, rule_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn remove_subscribe_ref(
        &self,
        device_id: &Uuid,
        group_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self.devices.get(device_id) {
            Some(device) => device.remove_subscribe_ref(group_id, rule_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    // pub async fn create_sink(
    //     &self,
    //     device_id: Uuid,
    //     sink_id: Option<Uuid>,
    //     req: CreateUpdateSinkReq,
    // ) -> HaliaResult<()> {
    //     match self.devices.get_mut(&device_id) {
    //         Some(mut device) => device.create_sink(sink_id, req).await,
    //         None => Err(HaliaError::NotFound),
    //     }
    // }

    // pub async fn search_sinks(
    //     &self,
    //     device_id: Uuid,
    //     page: usize,
    //     size: usize,
    // ) -> HaliaResult<SearchSinksResp> {
    //     match self.devices.get(&device_id) {
    //         Some(device) => Ok(device.search_sinks(page, size).await),
    //         None => Err(HaliaError::NotFound),
    //     }
    // }

    // pub async fn update_sink(
    //     &self,
    //     device_id: Uuid,
    //     sink_id: Uuid,
    //     req: CreateUpdateSinkReq,
    // ) -> HaliaResult<()> {
    //     match self.devices.get_mut(&device_id) {
    //         Some(mut device) => device.update_sink(sink_id, req).await,
    //         None => Err(HaliaError::NotFound),
    //     }
    // }

    // pub async fn delete_sink(&self, device_id: Uuid, sink_id: Uuid) -> HaliaResult<()> {
    //     match self.devices.get_mut(&device_id) {
    //         Some(mut device) => device.delete_sink(sink_id).await,
    //         None => Err(HaliaError::NotFound),
    //     }
    // }

    // pub async fn pre_publish(
    //     &self,
    //     device_id: &Uuid,
    //     sink_id: &Uuid,
    //     rule_id: &Uuid,
    // ) -> HaliaResult<()> {
    //     match self.devices.get_mut(&device_id) {
    //         Some(mut device) => device.pre_publish(sink_id, rule_id),
    //         None => Err(HaliaError::NotFound),
    //     }
    // }

    // pub async fn publish(
    //     &self,
    //     device_id: &Uuid,
    //     sink_id: &Uuid,
    //     rule_id: &Uuid,
    // ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
    //     match self.devices.get_mut(&device_id) {
    //         Some(mut device) => device.publish(sink_id, rule_id),
    //         None => Err(HaliaError::NotFound),
    //     }
    // }

    // pub async fn pre_unpublish(
    //     &self,
    //     device_id: &Uuid,
    //     sink_id: &Uuid,
    //     rule_id: &Uuid,
    // ) -> HaliaResult<()> {
    //     match self.devices.get_mut(&device_id) {
    //         Some(mut device) => device.pre_unpublish(sink_id, rule_id),
    //         None => Err(HaliaError::NotFound),
    //     }
    // }

    // pub async fn unpublish(
    //     &self,
    //     device_id: &Uuid,
    //     sink_id: &Uuid,
    //     rule_id: &Uuid,
    // ) -> HaliaResult<()> {
    //     match self.devices.get_mut(&device_id) {
    //         Some(mut device) => device.unpublish(sink_id, rule_id),
    //         None => Err(HaliaError::NotFound),
    //     }
    // }
}
