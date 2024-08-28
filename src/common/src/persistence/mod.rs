use std::{
    fmt::Display,
    fs::Permissions,
    io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};
use uuid::Uuid;

pub mod local;

use crate::error::HaliaResult;

pub struct Device {
    id: String,
    status: u8,
    conf: String,
}

pub struct SourceOrSink {
    id: String,
    // device or app id
    parent_id: String,
    conf: String,
}

pub trait Persistence {
    fn init(&self) -> HaliaResult<()>;

    fn create_device(&self, id: &uuid::Uuid, conf: String) -> HaliaResult<()>;
    fn read_devices(&self) -> HaliaResult<Vec<Device>>;
    fn update_device_status(&self, id: &uuid::Uuid, status: bool) -> HaliaResult<()>;
    fn update_device_conf(&self, id: &uuid::Uuid, conf: String) -> HaliaResult<()>;
    fn delete_device(&self, id: &uuid::Uuid) -> HaliaResult<()>;

    fn create_source(&self, parent_id: &Uuid, id: &Uuid, conf: String) -> HaliaResult<()>;
    fn read_sources(&self, parent_id: &Uuid) -> HaliaResult<Vec<SourceOrSink>>;
    fn update_source(&self, id: &Uuid, conf: String) -> HaliaResult<()>;
    fn delete_source(&self, id: &Uuid) -> HaliaResult<()>;

    fn create_sink(&self, parent_id: &Uuid, id: &Uuid, conf: String) -> HaliaResult<()>;
    fn read_sinks(&self, parent_id: &Uuid) -> HaliaResult<Vec<SourceOrSink>>;
    fn update_sink(&self, id: &Uuid, conf: String) -> HaliaResult<()>;
    fn delete_sink(&self, id: &Uuid) -> HaliaResult<()>;

    fn create_app(&self, id: &uuid::Uuid, body: String) -> crate::error::HaliaResult<()>;
    fn read_apps(&self) -> crate::error::HaliaResult<Vec<String>>;

    fn update_app_status(&self, id: &uuid::Uuid, stauts: bool) -> crate::error::HaliaResult<()> {
        todo!()
    }

    fn update_app_conf(&self, id: &uuid::Uuid, conf: String) -> crate::error::HaliaResult<()> {
        todo!()
    }

    fn delete_app(&self, id: &uuid::Uuid) -> crate::error::HaliaResult<()> {
        todo!()
    }

    fn create_rule(&self, id: &uuid::Uuid, body: String) -> crate::error::HaliaResult<()> {
        todo!()
    }

    fn read_rules(&self) -> crate::error::HaliaResult<Vec<String>> {
        todo!()
    }

    fn update_rule_status(&self, id: &uuid::Uuid, stauts: bool) -> crate::error::HaliaResult<()> {
        todo!()
    }

    fn update_rule_conf(&self, id: &uuid::Uuid, body: String) -> crate::error::HaliaResult<()> {
        todo!()
    }

    fn delete_rule(&self, id: &uuid::Uuid) -> crate::error::HaliaResult<()> {
        todo!()
    }
}
