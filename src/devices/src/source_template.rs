use common::error::{HaliaError, HaliaResult};
use types::{
    devices::{
        CreateUpdateSourceOrSinkTemplateReq, DeviceType, QuerySourceOrSinkTemplateParams,
        SearchSourcesOrSinkTemplatesItemResp, SearchSourcesOrSinkTemplatesResp,
    },
    Pagination,
};

use crate::{modbus, GLOBAL_DEVICE_MANAGER};

pub async fn create(req: CreateUpdateSourceOrSinkTemplateReq) -> HaliaResult<()> {
    match req.device_type {
        DeviceType::Modbus => modbus::source_template::validate(&req.ext)?,
        DeviceType::Opcua => todo!(),
        DeviceType::Coap => todo!(),
    }

    let id = common::get_id();
    storage::device::source_sink_template::insert(&id, storage::SourceSinkType::Source, req)
        .await?;

    Ok(())
}

pub async fn search(
    pagination: Pagination,
    query: QuerySourceOrSinkTemplateParams,
) -> HaliaResult<SearchSourcesOrSinkTemplatesResp> {
    let (count, db_sources) = storage::device::source_sink_template::search(
        pagination,
        storage::SourceSinkType::Source,
        query,
    )
    .await?;

    let templates: Vec<_> = db_sources
        .into_iter()
        .map(|x| transer_db_source_sink_template_to_resp(x))
        .collect();

    Ok(SearchSourcesOrSinkTemplatesResp {
        total: count,
        data: templates,
    })
}

pub async fn update(id: String, req: CreateUpdateSourceOrSinkTemplateReq) -> HaliaResult<()> {
    // todo validate
    let old_conf = storage::device::source_sink_template::read_conf(&id).await?;
    let old_conf: serde_json::Value = serde_json::from_slice(&old_conf)?;
    if old_conf != req.ext {
        let sources = storage::device::source_sink::read_sources_by_template_id(&id).await?;
        for source in sources {
            if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&source.device_id) {
                let customize_conf: serde_json::Value = serde_json::from_slice(&source.conf)?;
                device
                    .update_template_source(&source.id, customize_conf, req.ext.clone())
                    .await?;
            }
        }
    }

    storage::device::source_sink_template::update_conf(&id, req).await?;
    Ok(())
}

pub async fn delete(id: String) -> HaliaResult<()> {
    if storage::device::source_sink::count_by_template_id(&id).await? > 0 {
        return Err(HaliaError::DeleteRefing);
    }
    storage::device::source_sink_template::delete_by_id(&id).await?;
    Ok(())
}

pub async fn create_sink_template(req: CreateUpdateSourceOrSinkTemplateReq) -> HaliaResult<()> {
    // match req.device_type {
    //     DeviceType::Modbus => modbus::sink_template::validate(&req.ext)?,
    //     DeviceType::Opcua => todo!(),
    //     DeviceType::Coap => todo!(),
    // }
    todo!()
}

fn transer_db_source_sink_template_to_resp(
    db_template: storage::device::source_sink_template::SourceSinkTemplate,
) -> SearchSourcesOrSinkTemplatesItemResp {
    SearchSourcesOrSinkTemplatesItemResp {
        id: db_template.id,
        name: db_template.name,
        desc: db_template
            .des
            .map(|desc| unsafe { String::from_utf8_unchecked(desc) }),
        device_type: db_template.device_type.try_into().unwrap(),
        conf: serde_json::from_slice(&db_template.conf).unwrap(),
    }
}
