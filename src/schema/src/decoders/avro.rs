use std::io::Cursor;

use anyhow::Result;
use apache_avro::{from_avro_datum, Reader, Schema};
use bytes::Bytes;
use common::error::HaliaResult;
use message::{Message, MessageBatch};
use tracing::warn;
use types::schema::AvroDecodeConf;

use crate::Decoder;

struct Avro;

pub(crate) fn validate_conf(conf: &serde_json::Value) -> Result<()> {
    let conf: AvroDecodeConf = serde_json::from_value(conf.clone())?;
    Schema::parse_str(&conf.schema)?;
    Ok(())
}

pub(crate) fn new() -> HaliaResult<Box<dyn Decoder>> {
    Ok(Box::new(Avro))
}

pub(crate) fn new_with_conf(conf: AvroDecodeConf) -> HaliaResult<Box<dyn Decoder>> {
    let schema = Schema::parse_str(&conf.schema).unwrap();
    Ok(Box::new(AvroWithSchema { schema }))
}

impl Decoder for Avro {
    fn decode(&self, data: Bytes) -> Result<MessageBatch> {
        let mut mb = MessageBatch::default();

        // let mut reader = Cursor::new(data);
        // let value = from_avro_datum(&schema, &mut reader, None)?;

        let reader = Reader::new(&data[..]).unwrap();
        for value in reader {
            match value {
                Ok(value) => {
                    let msg = Message::try_from(value)?;
                    mb.push_message(msg);
                }
                Err(e) => warn!("Error decoding avro: {:?}", e),
            }
        }

        Ok(mb)
    }
}

struct AvroWithSchema {
    schema: Schema,
}

impl Decoder for AvroWithSchema {
    fn decode(&self, data: Bytes) -> Result<MessageBatch> {
        let value = from_avro_datum(&self.schema, &mut Cursor::new(data), None)?;
        dbg!(&value);
        // let reader = Reader::with_schema(&self.schema, Cursor::new(data)).unwrap();
        let mut mb = MessageBatch::default();
        // for value in reader {
        // match value {
        // Ok(v) => {
        let msg = Message::try_from(value)?;
        mb.push_message(msg);
        // }
        // Err(e) => bail!(e),
        // }
        // }
        Ok(mb)
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::{
        to_avro_datum,
        types::{Record, Value},
        Days, Decimal, Duration, Millis, Months, Schema,
    };
    use num_bigint::ToBigInt;
    use types::schema::AvroDecodeConf;

    use super::new_with_conf;

    #[test]
    fn test_decode_avro_with_schema() {
        let raw_schema = r#"{
            "type": "record",
            "name": "test",
            "fields": [
              {
                "name": "decimal_fixed",
                "type": {
                  "type": "fixed",
                  "size": 2,
                  "name": "decimal"
                },
                "logicalType": "decimal",
                "precision": 4,
                "scale": 2
              },
              {
                "name": "decimal_var",
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 10,
                "scale": 3
              },
              {
                "name": "uuid",
                "type": "string",
                "logicalType": "uuid"
              },
              {
                "name": "date",
                "type": "int",
                "logicalType": "date"
              },
              {
                "name": "time_millis",
                "type": "int",
                "logicalType": "time-millis"
              },
              {
                "name": "time_micros",
                "type": "long",
                "logicalType": "time-micros"
              },
              {
                "name": "timestamp_millis",
                "type": "long",
                "logicalType": "timestamp-millis"
              },
              {
                "name": "timestamp_micros",
                "type": "long",
                "logicalType": "timestamp-micros"
              },
              {
                "name": "local_timestamp_millis",
                "type": "long",
                "logicalType": "local-timestamp-millis"
              },
              {
                "name": "local_timestamp_micros",
                "type": "long",
                "logicalType": "local-timestamp-micros"
              },
              {
                "name": "duration",
                "type": {
                  "type": "fixed",
                  "size": 12,
                  "name": "duration"
                },
                "logicalType": "duration"
              }
            ]
          }"#;

        let schema = Schema::parse_str(raw_schema).unwrap();
        let mut record = Record::new(&schema).unwrap();
        record.put(
            "decimal_fixed",
            Decimal::from(9936.to_bigint().unwrap().to_signed_bytes_be()),
        );
        record.put(
            "decimal_var",
            Decimal::from((-32442.to_bigint().unwrap()).to_signed_bytes_be()),
        );
        record.put(
            "uuid",
            uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
        );
        record.put("date", Value::Date(1));
        record.put("time_millis", Value::TimeMillis(2));
        record.put("time_micros", Value::TimeMicros(3));
        record.put("timestamp_millis", Value::TimestampMillis(4));
        record.put("timestamp_micros", Value::TimestampMicros(5));
        record.put("timestamp_nanos", Value::TimestampNanos(6));
        record.put("local_timestamp_millis", Value::LocalTimestampMillis(4));
        record.put("local_timestamp_micros", Value::LocalTimestampMicros(5));
        record.put("local_timestamp_nanos", Value::LocalTimestampMicros(6));
        record.put(
            "duration",
            Duration::new(Months::new(6), Days::new(7), Millis::new(8)),
        );
        let data = to_avro_datum(&schema, record).unwrap();

        let conf = AvroDecodeConf {
            schema: raw_schema.to_owned(),
        };

        let avro_decoder = new_with_conf(conf).unwrap();
        let mut mb = avro_decoder.decode(data.into()).unwrap();
        let message = mb.take_one_message().unwrap();
        dbg!("{:?}", message);
    }
}
