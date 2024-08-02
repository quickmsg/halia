mod abs;
mod acos;
mod acosh;
mod asin;
mod asinh;
mod atan;
mod atan2;
mod atanh;
mod cbrt;
mod ceil;
mod cos;
mod cosh;
mod degrees;
mod exp;
mod exp2;
mod floor;
mod ln;
mod log;
// mod power;
mod sin;

use anyhow::{bail, Result};
use message::Message;
use types::rules::functions::ComputerConf;

use crate::Function;

pub trait Computer: Sync + Send {
    fn compute(&self, message: &mut Message);
}

pub struct Node {
    computers: Vec<Box<dyn Computer>>,
}

pub fn new(confs: Vec<ComputerConf>) -> Result<Box<dyn Function>> {
    let mut computers: Vec<Box<dyn Computer>> = Vec::with_capacity(confs.len());
    for conf in confs {
        let computer = match conf.typ {
            types::rules::functions::ComputerType::Abs => abs::new(conf)?,
            types::rules::functions::ComputerType::Acos => acos::new(conf)?,
            types::rules::functions::ComputerType::Acosh => acosh::new(conf)?,
            types::rules::functions::ComputerType::Asin => asin::new(conf)?,
            types::rules::functions::ComputerType::Asinh => asinh::new(conf)?,
            types::rules::functions::ComputerType::Atan => atan::new(conf)?,
            types::rules::functions::ComputerType::Atan2 => atan2::new(conf)?,
            types::rules::functions::ComputerType::Atanh => atanh::new(conf)?,
            types::rules::functions::ComputerType::Cbrt => cbrt::new(conf)?,
            types::rules::functions::ComputerType::Ceil => ceil::new(conf)?,
            types::rules::functions::ComputerType::Cos => cos::new(conf)?,
            types::rules::functions::ComputerType::Cosh => cosh::new(conf)?,
            types::rules::functions::ComputerType::Degrees => degrees::new(conf)?,
            types::rules::functions::ComputerType::Exp => exp::new(conf)?,
            types::rules::functions::ComputerType::Exp2 => exp2::new(conf)?,
            types::rules::functions::ComputerType::Floor => floor::new(conf)?,
            types::rules::functions::ComputerType::Ln => ln::new(conf)?,
            types::rules::functions::ComputerType::Log => log::new(conf)?,
            types::rules::functions::ComputerType::Sin => sin::new(conf)?,
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
