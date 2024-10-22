use common::error::HaliaResult;
use types::devices::{CreateUpdateSourceOrSinkTemplateReq, DeviceType};

pub async fn create_sink_template(req: CreateUpdateSourceOrSinkTemplateReq) -> HaliaResult<()> {
    // match req.device_type {
    //     DeviceType::Modbus => modbus::sink_template::validate(&req.ext)?,
    //     DeviceType::Opcua => todo!(),
    //     DeviceType::Coap => todo!(),
    // }
    todo!()
}
