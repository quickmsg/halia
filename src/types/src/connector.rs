use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize, Debug, Serialize, Clone)]
pub struct CreateConnectorReq {
    pub r#type: String,
    pub name: String,
    pub conf: Value,
}
