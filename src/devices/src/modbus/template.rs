use common::error::{HaliaError, HaliaResult};
use tracing::debug;
use types::devices::device_template::modbus::Conf;

pub fn validate_conf(conf: serde_json::Value) -> HaliaResult<()> {
    let conf: Conf = serde_json::from_value(conf)?;
    debug!("{:?}", conf);
    match conf.link_type {
        types::devices::modbus::LinkType::Ethernet => {
            if conf.ethernet.is_none() {
                return Err(HaliaError::Common("Ethernet conf is required".to_string()));
            }
        }
        types::devices::modbus::LinkType::Serial => {
            if conf.serial.is_none() {
                return Err(HaliaError::Common("Serial conf is required".to_string()));
            }
        }
    }

    Ok(())
}
