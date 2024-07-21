use serde::{Deserialize, Serialize};

pub mod apps;
pub mod devices;
pub mod rules;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BaseConf {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,
}
