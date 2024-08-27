use common::error::HaliaResult;
use types::databoard::CreateUpdateDataboardReq;
use uuid::Uuid;

pub struct Databoard {}

impl Databoard {
    pub fn new(id: Uuid, req: CreateUpdateDataboardReq) -> Self {
        todo!()
    }

    pub fn search() -> Self {
        todo!()
    }

    pub fn update() -> HaliaResult<()> {
        todo!()
    }

    pub fn delete() -> HaliaResult<()> {
        todo!()
    }

    pub fn create_data() -> HaliaResult<()> {
        todo!()
    }

    pub async fn search_datas() -> HaliaResult<()> {
        todo!()
    }

    pub async fn update_data() -> HaliaResult<()> {
        todo!()
    }

    pub async fn delete_data() -> HaliaResult<()> {
        todo!()
    }

    pub async fn add_data_ref() -> HaliaResult<()> {
        todo!()
    }

    pub async fn get_data_tx() -> HaliaResult<()> {
        todo!()
    }

    pub async fn del_data_tx() -> HaliaResult<()> {
        todo!()
    }

    pub async fn del_data_ref() -> HaliaResult<()> {
        todo!()
    }
}
