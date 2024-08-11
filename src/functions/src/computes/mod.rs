use anyhow::Result;
use message::Message;
use types::rules::functions::ComputerConf;

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

pub fn new(conf: ComputerConf) -> Result<Box<dyn Function>> {
    let mut computers: Vec<Box<dyn Computer>> = Vec::with_capacity(conf.computers.len());
    for item_conf in conf.computers {
        let computer = match item_conf.typ {
            types::rules::functions::ComputerType::Number => number::new(item_conf)?,
            types::rules::functions::ComputerType::String => string::new(item_conf)?,
            types::rules::functions::ComputerType::Hash => hash::new(item_conf)?,
            types::rules::functions::ComputerType::Date => todo!(),
        };
        computers.push(computer);
    }
    Ok(Box::new(Node { computers }))
}

impl Function for Node {
    fn call(&self, message_batch: &mut message::MessageBatch) -> bool {
        let messages = message_batch.get_messages_mut();
        messages.iter_mut().for_each(|msg| {
            for computer in &self.computers {
                computer.compute(msg);
            }
        });

        true
    }
}
