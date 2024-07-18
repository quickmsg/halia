use common::{error::HaliaResult, persistence};
use types::devices::coap::{CreateUpdateGroupResourceReq, SearchGroupResourcesItemResp};
use uuid::Uuid;

#[derive(Debug)]
pub struct Resource {
    pub id: Uuid,
    conf: CreateUpdateGroupResourceReq,
}

impl Resource {
    pub async fn new(
        device_id: &Uuid,
        group_id: &Uuid,
        resource_id: Option<Uuid>,
        req: CreateUpdateGroupResourceReq,
    ) -> HaliaResult<Self> {
        let (resource_id, new) = match resource_id {
            Some(resource_id) => (resource_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::devices::coap::create_group_resource(
                device_id,
                group_id,
                &resource_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Resource {
            id: resource_id,
            conf: req,
        })
    }

    pub fn search(&self) -> SearchGroupResourcesItemResp {
        SearchGroupResourcesItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub async fn update(
        &mut self,
        device_id: &Uuid,
        group_id: &Uuid,
        req: CreateUpdateGroupResourceReq,
    ) -> HaliaResult<()> {
        persistence::devices::coap::update_group_resource(
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
        persistence::devices::coap::delete_group_resource(device_id, group_id, &self.id).await?;
        Ok(())
    }
}
