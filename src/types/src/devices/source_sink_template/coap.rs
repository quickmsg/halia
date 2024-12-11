use serde::{Deserialize, Serialize};

use crate::devices::device::coap::{CoapOption, SourceMethod};

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct SourceCustomizeConf {
    pub method: SourceMethod,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get: Option<GetCustomizeConf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observe: Option<ObserveCustomizeConf>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct GetCustomizeConf {
    pub interval: u64,
    pub path: String,
    pub querys: Vec<(String, String)>,
    pub options: Vec<(CoapOption, String)>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct ObserveCustomizeConf {
    pub path: String,
    pub options: Vec<(CoapOption, String)>,
}

pub struct SourceTemplateConf {}

pub struct GetTemplateConf {}

pub struct ObserveTemplateConf {}
