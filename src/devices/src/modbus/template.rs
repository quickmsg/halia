use common::error::{HaliaError, HaliaResult};
use types::devices::device_template::modbus::TemplateConf;

pub fn validate_device_template_conf(conf: serde_json::Value) -> HaliaResult<()> {
    let conf: TemplateConf = serde_json::from_value(conf)?;
    match conf.link_type {
        types::devices::device::modbus::LinkType::Ethernet => {
            if conf.ethernet.is_none() {
                return Err(HaliaError::Common("Ethernet conf is required".to_string()));
            }
        }
        types::devices::device::modbus::LinkType::Serial => {
            if conf.serial.is_none() {
                return Err(HaliaError::Common("Serial conf is required".to_string()));
            }
        }
    }

    Ok(())
}

pub fn validate_source_template_conf(conf: serde_json::Value) -> HaliaResult<()> {
    let conf: types::devices::source_group::modbus::TemplateConf = serde_json::from_value(conf)?;
    if conf.interval == 0 {
        return Err(HaliaError::Common(
            "Interval must be greater than 0".to_string(),
        ));
    }
    Ok(())
}

pub fn validate_device_customize_conf(conf: serde_json::Value) -> HaliaResult<()> {
    let conf: types::devices::device_template::modbus::CustomizeConf =
        serde_json::from_value(conf)?;

    if conf.ethernet.is_none() && conf.serial.is_none() {
        return Err(HaliaError::Common(
            "Ethernet or serial conf is required".to_string(),
        ));
    }

    Ok(())
}
