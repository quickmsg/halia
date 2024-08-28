use rusqlite::Connection;
use uuid::Uuid;

use crate::{error::HaliaResult, persistence::Device};

use super::{Persistence, SourceOrSink};

pub struct Local {
    conn: Connection,
}

impl Local {
    pub fn new(path: &str) -> HaliaResult<Self> {
        let conn = Connection::open(path)?;
        let local = Local { conn };
        local.init()?;
        Ok(local)
    }
}

impl Persistence for Local {
    fn init(&self) -> HaliaResult<()> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS devices (
                    id TEXT PRIMARY KEY,
                    status INTEGER NOT NULL,
                    conf TEXT NOT NULL
                )",
            (),
        )?;
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS sources (
                    id TEXT PRIMARY KEY,
                    device_id TEXT NOT NULL,
                    name TEXT NOT NULL
                )",
            (),
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS sinks (
                    id TEXT PRIMARY KEY,
                    device_id TEXT NOT NULL,
                    name TEXT NOT NULL
                )",
            (),
        )?;
        Ok(())
    }

    fn create_device(&self, id: &uuid::Uuid, conf: String) -> HaliaResult<()> {
        let mut stmt = self
            .conn
            .prepare("INSERT INTO devices (id, status, conf) VALUES (?1, ?2, ?3)")?;
        stmt.execute([id.to_string(), 0.to_string(), conf])?;

        Ok(())
    }

    fn read_devices(&self) -> crate::error::HaliaResult<Vec<Device>> {
        let mut stmt = self.conn.prepare("SELECT id, status, conf FROM devies")?;
        let rows = stmt.query_map([], |row| {
            Ok(Device {
                id: row.get(0)?,
                status: row.get(1)?,
                conf: row.get(2)?,
            })
        })?;

        let mut devices = vec![];
        for device_result in rows {
            devices.push(device_result?);
        }

        Ok(devices)
    }

    fn update_device_status(&self, id: &uuid::Uuid, status: bool) -> crate::error::HaliaResult<()> {
        let mut stmt = self
            .conn
            .prepare("UPDATE devices SET status = ?1 WHERE id = ?2")?;
        stmt.execute([status.to_string(), id.to_string()])?;
        Ok(())
    }

    fn update_device_conf(&self, id: &uuid::Uuid, conf: String) -> crate::error::HaliaResult<()> {
        let mut stmt = self
            .conn
            .prepare("UPDATE devices SET conf = ?1 WHERE id = ?2")?;
        stmt.execute([conf, id.to_string()])?;
        Ok(())
    }

    fn delete_device(&self, id: &uuid::Uuid) -> crate::error::HaliaResult<()> {
        let mut stmt = self.conn.prepare("DELETE devices WHERE id = ?1")?;
        stmt.execute([id.to_string()])?;
        Ok(())
    }

    fn create_source(&self, parent_id: &Uuid, id: &Uuid, conf: String) -> HaliaResult<()> {
        let mut stmt = self
            .conn
            .prepare("INSERT INTO sources VALUES (?1), (?2), (?3)")?;
        stmt.execute([id.to_string(), parent_id.to_string(), conf])?;

        Ok(())
    }

    fn read_sources(&self, parent_id: &Uuid) -> HaliaResult<Vec<SourceOrSink>> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM sources WHERE parent_id = ?0")?;
        let rows = stmt.query_map([parent_id.to_string()], |row| {
            Ok(SourceOrSink {
                id: row.get(0)?,
                parent_id: row.get(1)?,
                conf: row.get(2)?,
            })
        })?;

        let mut sources = vec![];
        for source_res in rows {
            sources.push(source_res?);
        }

        Ok(sources)
    }

    fn update_source(&self, id: &Uuid, conf: String) -> HaliaResult<()> {
        let mut stmt = self
            .conn
            .prepare("UPDATE sources SET conf = ?1 WHERE id = ?2")?;
        stmt.execute([conf, id.to_string()])?;
        Ok(())
    }

    fn delete_source(&self, id: &Uuid) -> HaliaResult<()> {
        let mut stmt = self.conn.prepare("DELETE sources WHERE id = ?1")?;
        stmt.execute([id.to_string()])?;
        Ok(())
    }

    fn create_sink(&self, parent_id: &Uuid, id: &Uuid, conf: String) -> HaliaResult<()> {
        let mut stmt = self
            .conn
            .prepare("INSERT INTO sinks VALUES (?1), (?2), (?3)")?;
        stmt.execute([id.to_string(), parent_id.to_string(), conf])?;

        Ok(())
    }

    fn read_sinks(&self, parent_id: &Uuid) -> HaliaResult<Vec<SourceOrSink>> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM sinks WHERE parent_id = ?1")?;
        let rows = stmt.query_map([parent_id.to_string()], |row| {
            Ok(SourceOrSink {
                id: row.get(0)?,
                parent_id: row.get(1)?,
                conf: row.get(2)?,
            })
        })?;

        let mut sinks = vec![];
        for sink_res in rows {
            sinks.push(sink_res?);
        }

        Ok(sinks)
    }

    fn update_sink(&self, id: &Uuid, conf: String) -> HaliaResult<()> {
        let mut stmt = self
            .conn
            .prepare("UPDATE sinks SET conf = ?1 WHERE id = ?2")?;
        stmt.execute([conf, id.to_string()])?;
        Ok(())
    }

    fn delete_sink(&self, id: &Uuid) -> HaliaResult<()> {
        let mut stmt = self.conn.prepare("DELETE sinks WHERE id = ?1")?;
        stmt.execute([id.to_string()])?;
        Ok(())
    }

    fn create_app(&self, id: &uuid::Uuid, body: String) -> crate::error::HaliaResult<()> {
        todo!()
    }

    fn read_apps(&self) -> crate::error::HaliaResult<Vec<String>> {
        todo!()
    }

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
