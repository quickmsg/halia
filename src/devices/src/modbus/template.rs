use common::error::{HaliaError, HaliaResult};
use types::devices::{
    device_template::modbus::TemplateConf,
    source_sink_template::modbus::{SinkTemplateConf, SourceTemplateConf},
};

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
    let _conf: SourceTemplateConf = serde_json::from_value(conf)?;
    Ok(())
}

pub fn validate_sink_template_conf(conf: serde_json::Value) -> HaliaResult<()> {
    let _conf: SinkTemplateConf = serde_json::from_value(conf)?;
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
