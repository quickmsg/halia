//! 事件类型主要用于记录主要资源（设备、应用）等的启动停止和异常以及恢复，方便客户排查系统问题。
use common::{error::HaliaResult, storage};
use types::{
    events::{EventType, QueryParams, ResourceType, SearchEventsItemResp, SearchEventsResp},
    Pagination,
};

pub async fn search_events(
    query_params: QueryParams,
    pagination: Pagination,
) -> HaliaResult<SearchEventsResp> {
    let (count, db_events) = storage::event::search(pagination, query_params).await?;
    {
        let mut events = vec![];
        for db_event in db_events {
            // TODO remove unwrap
            let resource_type: ResourceType = db_event.resource_type.try_into().unwrap();
            let typ: EventType = db_event.typ.try_into().unwrap();
            events.push(SearchEventsItemResp {
                name: db_event.resource_name,
                resource_type,
                typ,
                info: db_event
                    .info
                    .map(|info| unsafe { String::from_utf8_unchecked(info) }),
            });
        }

        Ok(SearchEventsResp {
            total: count,
            data: events,
        })
    }
}
