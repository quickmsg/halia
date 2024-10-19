use common::error::HaliaResult;
use types::devices::modbus::SourceTemplateConf;

pub(crate) fn validate(conf: &serde_json::Value) -> HaliaResult<()> {
    let _conf: SourceTemplateConf = serde_json::from_value(conf.clone())?;
    Ok(())
}