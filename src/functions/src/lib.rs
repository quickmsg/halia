use anyhow::{bail, Result};
use message::MessageBatch;

// mod aggregate;
mod filter;
mod field;
pub mod merge;

pub trait Function: Send + Sync {
    fn call(&self, message_batch: &mut MessageBatch);
}

// pub fn new(req: &CreateRuleNode) -> Result<Box<dyn Function>> {
//     match req.r#type.as_str() {
//         "field" => match req.name.as_ref().unwrap().as_str() {
//             // "watermark" => {
//             //     let watermark = Watermark::new(cgn.conf.clone())?;
//             //     Ok(Box::new(watermark))
//             // }
//             // "name" => {
//             //     let name = Name::new(cgn.conf.clone())?;
//             //     Ok(Box::new(name))
//             // }
//             _ => bail!("not support"),
//         },
//         "filter" => {
//             let filter = filter::Node::new(req.conf.clone())?;
//             Ok(Box::new(filter))
//         }
//         // "compute" => {
//         //     ComputeNode::new(cgn.conf.clone())
//         // }
//         // "aggregate" => {
//         //     let aggregate = AggregateNode::new(cgn.conf.clone())?;
//         //     Ok(Box::new(aggregate))
//         // }
//         _ => bail!("not support"),
//     }
// }
