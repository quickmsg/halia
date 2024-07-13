use axum::extract::{Path, Query};
use common::error::HaliaError;
use device::GLOBAL_DEVICE_MANAGER;
use types::{
    device::{
        device::{SearchDeviceResp, SearchSinksResp},
        group::SearchGroupResp,
        point::{CreatePointReq, SearchPointResp, WritePointValueReq},
    },
    SearchResp,
};
use uuid::Uuid;

use crate::{AppResp, DeleteIdsQuery, Pagination};

pub(crate) async fn search_devices(pagination: Query<Pagination>) -> AppResp<SearchDeviceResp> {
    AppResp::with_data(
        GLOBAL_DEVICE_MANAGER
            .search_devices(pagination.p, pagination.s)
            .await,
    )
}
