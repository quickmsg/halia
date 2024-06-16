use anyhow::{bail, Result};
use eq::Eq;
use gt::Gt;
use gte::Gte;
use lt::Lt;
use lte::Lte;
use message::Message;
use neq::Neq;
use serde::Deserialize;
use tracing::debug;

use crate::Function;

pub mod eq;
pub mod gt;
pub mod gte;
pub mod lt;
pub mod lte;
pub mod neq;

pub trait Filter: Sync + Send {
    fn filter(&self, message: &Message) -> bool;
}

pub struct Node {
    filters: Vec<Box<dyn Filter>>,
}

#[derive(Deserialize)]
struct Conf {
    r#type: String,
    conf: serde_json::Value,
}

impl Node {
    pub fn new(conf: serde_json::Value) -> Result<Self> {
        let confs: Vec<Conf> = serde_json::from_value(conf)?;
        let mut filters: Vec<Box<dyn Filter>> = Vec::with_capacity(confs.len());
        for conf in confs {
            match conf.r#type.as_str() {
                "gt" => {
                    let gt = Gt::new(conf.conf)?;
                    filters.push(Box::new(gt));
                }
                "gte" => {
                    let gte = Gte::new(conf.conf)?;
                    filters.push(Box::new(gte));
                }
                "lt" => {
                    let lt = Lt::new(conf.conf)?;
                    filters.push(Box::new(lt));
                }
                "lte" => {
                    let lte = Lte::new(conf.conf)?;
                    filters.push(Box::new(lte));
                }
                "eq" => {
                    let eq = Eq::new(conf.conf)?;
                    filters.push(Box::new(eq));
                }
                "neq" => {
                    let neq = Neq::new(conf.conf)?;
                    filters.push(Box::new(neq));
                }
                _ => bail!("not support"),
            }
        }
        Ok(Node { filters })
    }
}

impl Function for Node {
    fn call(&self, message_batch: &mut message::MessageBatch) {
        let messages = message_batch.get_messages_mut();
        debug!("{:?}", messages);
        messages.retain(|message| {
            for filter in &self.filters {
                if filter.filter(message) {
                    return true;
                }
            }
            false
        });
        debug!("{:?}", messages);
    }
}
