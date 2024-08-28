use uuid::Uuid;

pub mod local;

use crate::error::HaliaResult;

pub struct Device {
    pub id: String,
    pub status: u8,
    pub conf: String,
}

pub struct App {
    pub id: String,
    pub status: u8,
    pub conf: String,
}

pub struct SourceOrSink {
    pub id: String,
    pub conf: String,
}

pub struct Rule {
    pub id: String,
    pub status: u8,
    pub conf: String,
}

pub trait Persistence {
    fn init(&self) -> HaliaResult<()>;

    fn create_device(&self, id: &uuid::Uuid, conf: String) -> HaliaResult<()>;
    fn read_devices(&self) -> HaliaResult<Vec<Device>>;
    fn update_device_status(&self, id: &uuid::Uuid, status: bool) -> HaliaResult<()>;
    fn update_device_conf(&self, id: &uuid::Uuid, conf: String) -> HaliaResult<()>;
    fn delete_device(&self, id: &uuid::Uuid) -> HaliaResult<()>;

    fn create_app(&self, id: &uuid::Uuid, conf: String) -> HaliaResult<()>;
    fn read_apps(&self) -> HaliaResult<Vec<App>>;
    fn update_app_status(&self, id: &uuid::Uuid, stauts: bool) -> HaliaResult<()>;
    fn update_app_conf(&self, id: &uuid::Uuid, conf: String) -> HaliaResult<()>;
    fn delete_app(&self, id: &uuid::Uuid) -> HaliaResult<()>;

    fn create_source(&self, parent_id: &Uuid, id: &Uuid, conf: String) -> HaliaResult<()>;
    fn read_sources(&self, parent_id: &Uuid) -> HaliaResult<Vec<SourceOrSink>>;
    fn update_source(&self, id: &Uuid, conf: String) -> HaliaResult<()>;
    fn delete_source(&self, id: &Uuid) -> HaliaResult<()>;

    fn create_sink(&self, parent_id: &Uuid, id: &Uuid, conf: String) -> HaliaResult<()>;
    fn read_sinks(&self, parent_id: &Uuid) -> HaliaResult<Vec<SourceOrSink>>;
    fn update_sink(&self, id: &Uuid, conf: String) -> HaliaResult<()>;
    fn delete_sink(&self, id: &Uuid) -> HaliaResult<()>;

    fn create_rule(&self, id: &uuid::Uuid, body: String) -> HaliaResult<()>;
    fn read_rules(&self) -> HaliaResult<Vec<Rule>>;
    fn update_rule_status(&self, id: &uuid::Uuid, stauts: bool) -> HaliaResult<()>;
    fn update_rule_conf(&self, id: &uuid::Uuid, body: String) -> HaliaResult<()>;
    fn delete_rule(&self, id: &uuid::Uuid) -> HaliaResult<()>;
}
