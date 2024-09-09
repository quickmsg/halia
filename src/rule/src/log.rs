use std::{
    fs::{File, OpenOptions},
    io::Write,
};

use anyhow::Result;
use uuid::Uuid;

pub struct Logger {
    file: File,
}

impl Logger {
    pub fn new(rule_id: &Uuid) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(rule_id.to_string())?;

        Ok(Logger { file })
    }

    pub fn log(&mut self, name: &str, message: &str) -> Result<()> {
        self.file
            .write_all(format!("{} {}", name, message).as_bytes())?;

        Ok(())
    }
}