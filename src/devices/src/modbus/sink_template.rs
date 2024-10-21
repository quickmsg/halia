use common::error::HaliaResult;
use types::devices::modbus::SinkTemplateConf;

pub(crate) fn validate(conf: &serde_json::Value) -> HaliaResult<()> {
    let _conf: SinkTemplateConf = serde_json::from_value(conf.clone())?;
    Ok(())
}
