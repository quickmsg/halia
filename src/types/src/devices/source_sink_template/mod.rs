use serde::{Deserialize, Serialize};

use crate::BaseConf;

use super::{ConfType, DeviceType};

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateUpdateReq {
    pub device_type: DeviceType,
    pub conf_type: ConfType,
    pub template_id: Option<String>,
    pub base: BaseConf,
    pub conf: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    pub device_type: Option<DeviceType>,
}