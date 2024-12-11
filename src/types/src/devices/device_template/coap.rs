use std::num::NonZeroU16;

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CustomizeConf {
    pub host: String,
    pub port: NonZeroU16,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TemplateConf {}