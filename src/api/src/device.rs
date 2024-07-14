use axum::extract::Query;
use device::GLOBAL_DEVICE_MANAGER;
use types::device::device::SearchDeviceResp;

use crate::{AppResp, Pagination};

pub(crate) async fn search_devices(pagination: Query<Pagination>) -> AppResp<SearchDeviceResp> {
    AppResp::with_data(
        GLOBAL_DEVICE_MANAGER
            .search(pagination.p, pagination.s)
            .await,
    )
}
