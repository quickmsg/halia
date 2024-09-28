use common::error::HaliaResult;
use types::apps::influxdb::SinkConf;

pub struct Sink {
    pub id: String,
    pub conf: serde_json::Value,
}

impl Sink {
    pub fn new(id: String, conf: serde_json::Value) -> Self {
        Sink { id, conf }
    }

    pub fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }
}
