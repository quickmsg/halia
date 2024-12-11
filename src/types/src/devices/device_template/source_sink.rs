use serde::{Deserialize, Serialize};

use crate::devices::ConfType;

#[derive(Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
}

#[derive(Serialize)]
pub struct ListResp {
    pub count: usize,
    pub list: Vec<ListItem>,
}

#[derive(Serialize)]
pub struct ListItem {
    pub id: String,
    pub name: String,
}

#[derive(Serialize)]
pub struct ReadResp {
    pub id: String,
    pub name: String,
    pub conf_type: ConfType,
    pub template_id: Option<String>,
    pub conf: serde_json::Value,
}
