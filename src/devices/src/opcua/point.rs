use common::error::HaliaResult;
use opcua::types::{DataValue, NodeId, Variant};
use types::device::point::CreatePointReq;
use uuid::Uuid;

#[derive(Debug)]
pub struct Point {
    pub id: Uuid,
    pub name: String,
    pub node_id: NodeId,
    pub desc: Option<String>,
    pub value: Option<Variant>,
}

impl Point {
    pub fn new(id: Uuid, req: CreatePointReq) -> HaliaResult<Point> {
        let node_id: NodeId = serde_json::from_value(req.conf)?;
        Ok(Point {
            id,
            name: req.name,
            node_id,
            desc: req.desc,
            value: Some(Variant::Empty),
        })
    }

    pub async fn update(&mut self, req: &CreatePointReq) -> HaliaResult<()> {
        todo!()
    }

    pub fn write(&mut self, data_value: Option<DataValue>) {
        match data_value {
            Some(data_value) => self.value = data_value.value,
            None => self.value = None,
        }
    }
}
