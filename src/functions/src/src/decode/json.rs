use anyhow::{bail, Result};
use message::{json::message::JsonMessage, Message};
use tracing::{debug, error};
use types::graph::Node;

pub struct JsonDecoder {}

impl JsonDecoder {
    // TODO schema
    pub fn new() -> Result<JsonDecoder> {
        Ok(JsonDecoder {})
    }

    // fn decode(&self, payload: Bytes) -> Result<Message> {
    //     let json_message = JsonMessage::new(payload)?;
    //     Ok(Message::Json(json_message))
    // }

    // pub async fn run(&mut self) {
    //     debug!("json decoder run");
    //     loop {
    //         tokio::select! {
    //           bytes = self.rx.recv() => {
    //             if let Ok(bytes) = bytes {
    //             let msg = self.decode(bytes);
    //             match msg {
    //                 Ok(msg) => match self.tx.send(msg) {
    //                     Ok(_) => {},
    //                     Err(e) => error!("send err:{}", e),
    //                 }
    //                 Err(e) => error!("decode err:{}", e),
    //             }
    //             }
    //           }
    //         }
    //     }
    // }
}

pub fn decode(message: &Message) -> Result<Message> {
    match message {
        Message::Raw(raw) => match JsonMessage::new(raw.value.clone()) {
            Ok(json) => Ok(Message::Json(json)),
            Err(e) => bail!(e),
        },
        _ => bail!("already decode"),
    }
}

// impl Node for JsonDecoder {
//     fn operate(&self, message: &mut message::Message) -> Result<()> {
//         match message {
//             message::Message::Json(_) => todo!(),
//             message::Message::Raw(raw) => {
//                 let json_message = JsonMessage::new(raw.value.clone())?;
//                 message = &mut Message::Json(json_message);
//             }
//         }

//         Ok(())
//     }
// }
