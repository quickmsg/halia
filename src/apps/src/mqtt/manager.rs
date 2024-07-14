use dashmap::DashMap;
use uuid::Uuid;

use super::MqttClient;

pub struct Manager {
    apps: DashMap<Uuid, MqttClient>,
}
