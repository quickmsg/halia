use anyhow::Result;
use async_trait::async_trait;
use message::Message;
use types::rules::functions::computer::Conf;

use crate::Function;

mod hash;
mod number;
mod string;

pub trait Computer: Sync + Send {
    fn compute(&self, message: &mut Message);
}

pub struct Node {
    computers: Vec<Box<dyn Computer>>,
}

pub fn new(conf: Conf) -> Result<Box<dyn Function>> {
    let mut computers: Vec<Box<dyn Computer>> = Vec::with_capacity(conf.items.len());
    for item_conf in conf.items {
        let computer = match item_conf.typ {
            types::rules::functions::computer::Type::Number => {
                // TODO remove unwrap
                number::new(item_conf.number.unwrap())?
            }
            types::rules::functions::computer::Type::String => {
                string::new(item_conf.string.unwrap())?
            }
            types::rules::functions::computer::Type::Hash => hash::new(item_conf.hash.unwrap())?,
            // types::rules::functions::computer::Type::Date => date::new(item_conf)?,
            _ => todo!(),
        };
        computers.push(computer);
    }
    Ok(Box::new(Node { computers }))
}

#[async_trait]
impl Function for Node {
    async fn call(&self, message_batch: &mut message::MessageBatch) -> bool {
        let messages = message_batch.get_messages_mut();
        messages.iter_mut().for_each(|msg| {
            for computer in &self.computers {
                computer.compute(msg);
            }
        });

        true
    }
}
