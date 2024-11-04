use common::error::HaliaResult;
use types::devices::device_template::opcua::TemplateConf;

pub fn validate_device_template_conf(conf: serde_json::Value) -> HaliaResult<()> {
    let _: TemplateConf = serde_json::from_value(conf)?;
    Ok(())
}
