use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct MqttServerConf {
    pub router: RouterConf,
    pub v4: ServerSettings,
    pub v5: ServerSettings,
}

#[derive(Deserialize, Serialize)]
pub struct ServerSettings {
    pub name: String,
    pub listen: SocketAddr,
    pub next_connection_delay_ms: u64,
    pub connection_timeout_ms: u16,
    pub max_payload_size: usize,
    pub max_inflight_count: usize,
}

#[derive(Deserialize, Serialize)]
pub struct RouterConf {
    pub max_connections: usize,
    pub max_outgoing_packet_count: u64,
    pub max_segment_size: usize,
    pub max_segment_count: usize,
}
