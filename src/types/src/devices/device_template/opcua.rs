use serde::{Deserialize, Serialize};

use crate::devices::device::opcua::{AuthCertificate, AuthMethod, AuthUsername};

#[derive(Debug, Serialize, Deserialize)]
pub struct CustomizeConf {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TemplateConf {
    pub path: String,
    pub reconnect: u64, // 秒
    pub auth_method: AuthMethod,
    pub auth_username: Option<AuthUsername>,
    pub auth_certificate: Option<AuthCertificate>,
}