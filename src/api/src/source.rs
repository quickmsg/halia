use axum::{extract::Path, Json};
use managers::source::GLOBAL_SOURCE_MANAGER;
use types::source::{CreateSourceReq, ListSourceResp, SourceDetailResp};
use uuid::Uuid;

use crate::AppResp;

pub(crate) async fn create_source(Json(req): Json<CreateSourceReq>) -> AppResp<()> {
    match GLOBAL_SOURCE_MANAGER.create_source(None, req).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

// pub(crate) async fn read_source(Path(id): Path<Uuid>) -> AppResp<SourceDetailResp> {
//     match GLOBAL_SOURCE_MANAGER.read_source(id).await {
//         Ok(resp) => AppResp::with_data(resp),
//         Err(e) => e.into(),
//     }
// }

// pub(crate) async fn read_sources() -> AppResp<Vec<ListSourceResp>> {
//     AppResp::with_data(GLOBAL_SOURCE_MANAGER.read_sources().await)
// }

pub(crate) async fn delete_source() {

}
