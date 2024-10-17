use std::io::Cursor;

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use message::{Message, MessageBatch, MessageValue};
use types::schema::CsvDecodeConf;

use crate::Decoder;

pub struct Csv {
    has_headers: bool,
    headers: Option<Vec<String>>,
}

pub(crate) fn validate_conf(conf: &serde_json::Value) -> Result<()> {
    let conf: CsvDecodeConf = serde_json::from_value(conf.clone())?;
    Ok(())
}

impl Csv {
    pub fn new() -> Self {
        Self {
            headers: None,
            has_headers: false,
        }
    }

    pub fn set_has_headers(&mut self, has_headers: bool) {
        self.has_headers = has_headers;
    }

    pub fn set_headers(&mut self, headers: Vec<String>) {
        self.headers = Some(headers);
    }

    fn get_field_name(&self, index: usize) -> String {
        if let Some(headers) = &self.headers {
            if let Some(header) = headers.get(index) {
                return header.clone();
            }
        }

        format!("t{}", index)
    }

    pub fn mb_to_payload(&self, mb: MessageBatch) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        for message in mb.get_messages().iter() {
            let mut index = 0;
            loop {
                let field = self.get_field_name(index);
                match message.get(&field) {
                    Some(value) => buf.put(value.to_string().as_bytes()),
                    None => break,
                }
                index += 1;
            }

            buf.put(&b"\n"[..]);
        }

        Ok(buf.to_vec().into())
    }
}

impl Decoder for Csv {
    fn decode(&self, data: Bytes) -> Result<MessageBatch> {
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(self.has_headers)
            .from_reader(Cursor::new(data));

        let mut mb = MessageBatch::default();

        if let Some(result) = rdr.records().next() {
            let record = result?;
            let mut message = Message::default();

            for (index, field) in record.iter().enumerate() {
                message.add(
                    self.get_field_name(index),
                    MessageValue::String(field.to_owned()),
                );
            }
            mb.push_message(message);
        }

        Ok(mb)
    }
}

// #[cfg(test)]
// mod tests {
//     use tracing::debug;

//     use super::*;

//     #[cfg(test)]
//     fn test_payload_to_mb() {
//         let headers = vec![
//             "a".to_owned(),
//             "b".to_owned(),
//             "c".to_owned(),
//             "d".to_owned(),
//         ];
//         let csv = Csv::new(Some(headers), false);
//         let payload = b"1,2,3,4";
//         let mut result = csv.payload_to_mb(Bytes::from_static(payload)).unwrap();

//         let msg = result.take_one_message().unwrap();
//         debug!("{:?}", msg);

//         // let mut mb = MessageBatch::default();
//         // let mut msg = Message::default();
//         // msg.add("a".to_owned(), MessageValue::String("1".to_owned()));
//         // msg.add("b".to_owned(), MessageValue::String("2".to_owned()));
//         // msg.add("c".to_owned(), MessageValue::String("3".to_owned()));
//         // msg.add("d".to_owned(), MessageValue::String("4".to_owned()));
//         // mb.push_message(msg);
//     }
// }
