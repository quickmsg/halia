use anyhow::{bail, Result};
use message::{Message, MessageValue};
use tracing::debug;
use types::rules::functions::FilterConf;

use crate::Function;

mod ct;
mod eq;
mod gt;
mod gte;
mod neq;
mod reg;

pub trait Filter: Sync + Send {
    fn filter(&self, message: &Message) -> bool;
}

pub struct Node {
    filters: Vec<Box<dyn Filter>>,
}

pub fn new(confs: Vec<FilterConf>) -> Result<Box<dyn Function>> {
    let mut filters: Vec<Box<dyn Filter>> = Vec::with_capacity(confs.len());
    for conf in confs {
        match conf.typ.as_str() {
            gt::TYPE => filters.push(gt::new(conf)?),
            gte::TYPE => filters.push(gte::new(conf)?),
            // "lt" => {
            //     let lt = Lt::new(conf.conf)?;
            //     filters.push(Box::new(lt));
            // }
            // "lte" => {
            //     let lte = Lte::new(conf.conf)?;
            //     filters.push(Box::new(lte));
            // }
            eq::TYPE => filters.push(eq::new(conf)?),
            neq::TYPE => filters.push(neq::new(conf)?),
            // ct::TYPE => filters.push(ct::new(conf)?),
            // reg::TYPE => filters.push(reg::new(conf)?),
            _ => bail!("not support"),
        }
    }
    Ok(Box::new(Node { filters }))
}

impl Function for Node {
    fn call(&self, message_batch: &mut message::MessageBatch) -> bool {
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

        message_batch.len() != 0
    }
}

pub(crate) fn get_target(conf: &FilterConf) -> Result<(Option<MessageValue>, Option<String>)> {
    match &conf.value.typ {
        types::TargetValueType::Const => {
            Ok((Some(MessageValue::from(conf.value.value.clone())), None))
        }
        types::TargetValueType::Variable => match conf.value.value.as_str() {
            Some(str) => Ok((None, Some(str.to_string()))),
            None => bail!("动态值错误"),
        },
    }
}
