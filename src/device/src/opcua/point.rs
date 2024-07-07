use common::error::HaliaResult;
use opcua::types::NodeId;
use serde_json::Value;
use types::device::point::CreatePointReq;
use uuid::Uuid;

#[derive(Debug)]
pub struct Point {
    pub id: Uuid,
    pub name: String,
    pub node_id: NodeId,
    pub desc: Option<String>,
    pub value: Value,
}

impl Point {
    pub fn new(id: Uuid, req: CreatePointReq) -> HaliaResult<Point> {
        let node_id: NodeId = serde_json::from_value(req.conf)?;
        Ok(Point {
            id,
            name: req.name,
            node_id,
            desc: req.desc,
            value: Value::Null,
        })
    }

    pub async fn update(&mut self, req: &CreatePointReq) -> HaliaResult<()> {
        todo!()
    }
}
