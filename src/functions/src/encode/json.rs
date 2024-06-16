use anyhow::{bail, Result};
use message::{raw::message::RawMessage, Message};

pub fn encode(message: &Message) -> Result<Message> {
    match message {
        Message::Json(json_message) => match json_message.encode() {
            Ok(msg) => Ok(RawMessage::new(msg)),
            Err(e) => bail!("json_message encode err:{}", e),
        },
        _ => bail!("already decode"),
    }
}
