use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::BaseConf;

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct CreateUpdateDataboardReq {
    pub base: BaseConf,
}

#[derive(Serialize)]
pub struct SearchDataboardsResp {
    pub id: Uuid,
    pub base: BaseConf,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct CreateUpdateDataReq {
    pub base: BaseConf,
}

#[derive(Serialize)]
pub struct SearchDatasResp {
    pub id: Uuid,
    pub base: BaseConf,
}
