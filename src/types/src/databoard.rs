use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{BaseConf, RuleRef};

#[derive(Serialize)]
pub struct Summary {
    pub total: usize,
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct CreateUpdateDataboardReq {
    pub base: BaseConf,
    pub ext: DataboardConf,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct DataboardConf {}

#[derive(Serialize)]
pub struct SearchDataboardsResp {
    pub total: usize,
    pub data: Vec<SearchDataboardsItemResp>,
}

#[derive(Serialize)]
pub struct SearchDataboardsItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateDataboardReq,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct CreateUpdateDataReq {
    pub base: BaseConf,
    pub ext: DataConf,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct DataConf {}

#[derive(Serialize)]
pub struct SearchDatasResp {
    pub total: usize,
    pub data: Vec<SearchDatasItemResp>,
}

#[derive(Serialize)]
pub struct SearchDatasItemResp {
    #[serde(flatten)]
    pub info: SearchDatasInfoResp,
    pub rule_ref: RuleRef,
}

#[derive(Serialize)]
pub struct SearchDatasInfoResp {
    pub id: Uuid,
    pub conf: CreateUpdateDataReq,
}
