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
            //     types::rules::functions::ComputerType::Abs => abs::new(conf)?,
            //     types::rules::functions::ComputerType::Acos => acos::new(conf)?,
            //     types::rules::functions::ComputerType::Acosh => acosh::new(conf)?,
            //     types::rules::functions::ComputerType::Asin => asin::new(conf)?,
            //     types::rules::functions::ComputerType::Asinh => asinh::new(conf)?,
            //     types::rules::functions::ComputerType::Atan => atan::new(conf)?,
            //     types::rules::functions::ComputerType::Atan2 => atan2::new(conf)?,
            //     types::rules::functions::ComputerType::Atanh => atanh::new(conf)?,
            //     types::rules::functions::ComputerType::Cbrt => cbrt::new(conf)?,
            //     types::rules::functions::ComputerType::Ceil => ceil::new(conf)?,
            //     types::rules::functions::ComputerType::Cos => cos::new(conf)?,
            //     types::rules::functions::ComputerType::Cosh => cosh::new(conf)?,
            //     types::rules::functions::ComputerType::Degrees => degrees::new(conf)?,
            //     types::rules::functions::ComputerType::Exp => exp::new(conf)?,
            //     types::rules::functions::ComputerType::Exp2 => exp2::new(conf)?,
            //     types::rules::functions::ComputerType::Floor => floor::new(conf)?,
            //     types::rules::functions::ComputerType::Ln => ln::new(conf)?,
            //     types::rules::functions::ComputerType::Log => log::new(conf)?,
            //     types::rules::functions::ComputerType::Sin => sin::new(conf)?,
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
