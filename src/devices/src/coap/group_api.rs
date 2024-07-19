use std::net::SocketAddr;

use common::{error::HaliaResult, persistence};
use protocol::coap::request::{CoapRequest, Method, RequestBuilder};
use types::devices::coap::{CreateUpdateGroupAPIReq, SearchGroupAPIsItemResp};
use uuid::Uuid;

#[derive(Debug)]
pub struct API {
    pub id: Uuid,
    conf: CreateUpdateGroupAPIReq,
    pub request: CoapRequest<SocketAddr>,
}

impl API {
    pub async fn new(
        device_id: &Uuid,
        group_id: &Uuid,
        api_id: Option<Uuid>,
        req: CreateUpdateGroupAPIReq,
    ) -> HaliaResult<Self> {
        let (api_id, new) = match api_id {
            Some(api_id) => (api_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::devices::coap::create_group_api(
                device_id,
                group_id,
                &api_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        let request = RequestBuilder::new(&req.path, Method::Get)
            // .queries(todo!())
            .domain(req.domain.clone())
            .build();

        Ok(Self {
            id: api_id,
            conf: req,
            request,
        })
    }

    pub fn search(&self) -> SearchGroupAPIsItemResp {
        SearchGroupAPIsItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub async fn update(
        &mut self,
        device_id: &Uuid,
        group_id: &Uuid,
        req: CreateUpdateGroupAPIReq,
    ) -> HaliaResult<()> {
        persistence::devices::coap::update_group_api(
            device_id,
            group_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        self.conf = req;

        Ok(())
    }

    pub async fn delete(&self, device_id: &Uuid, group_id: &Uuid) -> HaliaResult<()> {
        persistence::devices::coap::delete_group_api(device_id, group_id, &self.id).await?;
        Ok(())
    }
}
