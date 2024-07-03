use std::sync::{atomic::AtomicBool, Arc, Mutex};

use common::error::HaliaResult;
use group::Group;
use tokio::sync::RwLock;
use types::device::device::CreateDeviceReq;
use uuid::Uuid;

use crate::Device;

mod group;
mod point;

struct OpcUa {
    id: Uuid,
    on: Arc<AtomicBool>,
    err: Arc<AtomicBool>,
    conf: Arc<Mutex<Conf>>,
    groups: Arc<RwLock<Vec<Group>>>,
}

struct Conf {
    url: String,
    password: Option<Password>,
}

struct Password {
    username: String,
    password: String,
}

pub(crate) fn new(id: Uuid, req: &CreateDeviceReq) -> HaliaResult<Box<dyn Device>> {
    todo!()
}
