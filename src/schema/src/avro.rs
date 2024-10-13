use std::io::Cursor;

use anyhow::Error;
use anyhow::Result;
use apache_avro::{Reader, Schema};
use bytes::Bytes;
use message::MessageBatch;
use message::MessageValue;

use crate::Coder;

pub struct AvroNoSchema;

pub struct Avro {
    has_schema: bool,
    schema: Schema,
}

pub fn new(schema: String) -> Avro {
    let schema = Schema::parse_str(&schema).unwrap();
    Avro {
        has_schema: false,
        schema,
    }
}

impl Coder for Avro {
    fn decode(&self, data: Bytes) -> Result<MessageBatch> {
        // let reader = Reader::with_schema(&self.schema, Cursor::new(data)).unwrap();
        // let mut mb = MessageBatch::default();
        // for value in reader {
        //     let value = value?;
        //     let mut message = Message::default();
        //     for (key, value) in value {
        //         message.add(key, value);
        //     }
        //     mb.push_message(message);
        // }
        // Ok(mb)
        todo!()
    }

    fn encode(&self, mb: MessageBatch) -> Result<Bytes> {
        todo!()
    }
}

impl Avro {
    fn decode(&self, data: Bytes) -> Result<Vec<u8>, Error> {
        let reader = Reader::with_schema(&self.schema, Cursor::new(data)).unwrap();
        for value in reader {
            let value = value?;
            println!("{:?}", value);
        }
        todo!()
    }

    fn encode(&self, data: Vec<u8>) -> Result<Bytes, Error> {
        todo!()
    }
}

// fn json_to_avro(json_value: &JsonValue, schema: &Schema) -> Result<AvroValue, Box<dyn std::error::Error>> {
//     match json_value {
//         JsonValue::Null => Ok(AvroValue::Null),
//         JsonValue::Bool(b) => Ok(AvroValue::Boolean(*b)),
//         JsonValue::Number(num) => {
//             if let Some(n) = num.as_i64() {
//                 Ok(AvroValue::Long(n))
//             } else if let Some(n) = num.as_f64() {
//                 Ok(AvroValue::Double(n))
//             } else {
//                 Err("Unsupported number type".into())
//             }
//         },
//         JsonValue::String(s) => Ok(AvroValue::String(s.clone())),
//         JsonValue::Array(arr) => {
//             let avro_arr = arr.iter()
//                 .map(|item| json_to_avro(item, schema))
//                 .collect::<Result<Vec<_>, _>>()?;
//             Ok(AvroValue::Array(avro_arr))
//         },
//         JsonValue::Object(obj) => {
//             let mut avro_fields = Vec::new();
//             for (key, value) in obj.iter() {
//                 let field_value = json_to_avro(value, schema)?;
//                 avro_fields.push((key.clone(), field_value));
//             }
//             Ok(AvroValue::Record(avro_fields))
//         },
//     }
// }

// {
//     "type": "record",
//     "name": "test",
//     "fields": [
//         {"name": "a", "type": "long", "default": 42},
//         {"name": "b", "type": "string"}
//     ]
// }