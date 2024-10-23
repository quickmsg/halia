use common::error::{HaliaError, HaliaResult};
use tracing::debug;
use types::devices::{
    device_template::modbus::Conf,
    source_sink_template::modbus::{SinkTemplateConf, SourceTemplateConf},
};

pub fn validate_device_template_conf(conf: serde_json::Value) -> HaliaResult<()> {
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

pub fn validate_source_template_conf(conf: serde_json::Value) -> HaliaResult<()> {
    let _conf: SourceTemplateConf = serde_json::from_value(conf)?;
    Ok(())
}

pub fn validate_sink_template_conf(conf: serde_json::Value) -> HaliaResult<()> {
    let _conf: SinkTemplateConf = serde_json::from_value(conf)?;
    Ok(())
}
