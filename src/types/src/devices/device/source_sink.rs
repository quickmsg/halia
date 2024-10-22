use serde::{Deserialize, Serialize};

use crate::{devices::ConfType, BaseConf};

#[derive(Deserialize, Serialize, Clone, PartialEq)]
pub struct CreateUpdateReq {
    pub device_id: String,
    pub conf_type: ConfType,
    pub template_id: Option<String>,
    pub base: BaseConf,
    pub conf: serde_json::Value,
}

#[derive(Serialize)]
pub struct SearchResp {
    pub id: String,
}
