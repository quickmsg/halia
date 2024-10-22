use serde::{Deserialize, Serialize};

use crate::BaseConf;

use super::{ConfType, DeviceType};

pub mod source_sink;

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct CreateUpdateReq {
    pub device_type: DeviceType,
    pub conf_type: ConfType,
    pub template_id: Option<String>,
    pub base: BaseConf,
    pub conf: serde_json::Value,
}
