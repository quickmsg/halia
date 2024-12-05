use crate::{Message, MessageValue};
use anyhow::Result;

impl TryFrom<apache_avro::types::Value> for Message {
    type Error = anyhow::Error;

    fn try_from(value: apache_avro::types::Value) -> Result<Self, Self::Error> {
        Ok(Self {
            metadatas: Default::default(),
            value: MessageValue::try_from(value)?,
        })
    }
}

impl TryFrom<apache_avro::types::Value> for MessageValue {
    type Error = anyhow::Error;

    fn try_from(value: apache_avro::types::Value) -> std::result::Result<Self, Self::Error> {
        match value {
            apache_avro::types::Value::Null => Ok(Self::Null),
            apache_avro::types::Value::Boolean(b) => Ok(Self::Boolean(b)),
            apache_avro::types::Value::Int(i) => Ok(Self::Int64(i as i64)),
            apache_avro::types::Value::Long(l) => Ok(Self::Int64(l)),
            apache_avro::types::Value::Float(f) => Ok(Self::Float64(f as f64)),
            apache_avro::types::Value::Double(f) => Ok(Self::Float64(f)),
            apache_avro::types::Value::Bytes(vec) => Ok(Self::Bytes(vec)),
            apache_avro::types::Value::String(s) => Ok(Self::String(s)),
            apache_avro::types::Value::Fixed(_, vec) => todo!(),
            apache_avro::types::Value::Enum(_, _) => todo!(),
            apache_avro::types::Value::Union(_, value) => todo!(),
            apache_avro::types::Value::Array(vec) => Ok(Self::Array(
                vec.into_iter().map(Self::try_from).collect::<Result<_>>()?,
            )),
            apache_avro::types::Value::Map(hash_map) => Ok(Self::Object(
                hash_map
                    .into_iter()
                    // TODO unwrap
                    .map(|(key, value)| (key.into(), Self::try_from(value).unwrap()))
                    .collect(),
            )),
            apache_avro::types::Value::Record(vec) => Ok(Self::Object(
                vec.into_iter()
                    .map(|(key, value)| (key.into(), value.try_into().unwrap()))
                    .collect(),
            )),
            apache_avro::types::Value::Date(d) => Ok(Self::Int64(d as i64)),
            apache_avro::types::Value::Decimal(decimal) => {
                let vec: Vec<u8> = decimal.try_into()?;
                Ok(Self::Bytes(vec))
            }
            apache_avro::types::Value::BigDecimal(big_decimal) => todo!(),
            apache_avro::types::Value::TimeMillis(t) => Ok(Self::Int64(t as i64)),
            apache_avro::types::Value::TimeMicros(t) => Ok(Self::Int64(t as i64)),
            apache_avro::types::Value::TimestampMillis(t) => Ok(Self::Int64(t)),
            apache_avro::types::Value::TimestampMicros(t) => Ok(Self::Int64(t)),
            apache_avro::types::Value::TimestampNanos(t) => Ok(Self::Int64(t)),
            apache_avro::types::Value::LocalTimestampMillis(t) => Ok(Self::Int64(t)),
            apache_avro::types::Value::LocalTimestampMicros(t) => Ok(Self::Int64(t)),
            apache_avro::types::Value::LocalTimestampNanos(t) => Ok(Self::Int64(t)),
            apache_avro::types::Value::Duration(duration) => {
                let mills: u32 = duration.millis().try_into()?;
                Ok(Self::Int64(mills as i64))
            }
            apache_avro::types::Value::Uuid(uuid) => {
                Ok(self::MessageValue::String(uuid.to_string()))
            }
        }
    }
}

impl Into<apache_avro::types::Value> for MessageValue {
    fn into(self) -> apache_avro::types::Value {
        match self {
            MessageValue::Null => apache_avro::types::Value::Null,
            MessageValue::Boolean(b) => apache_avro::types::Value::Boolean(b),
            MessageValue::Int64(i) => apache_avro::types::Value::Long(i),
            MessageValue::Float64(f) => apache_avro::types::Value::Double(f),
            MessageValue::String(s) => apache_avro::types::Value::String(s),
            MessageValue::Bytes(vec) => apache_avro::types::Value::Bytes(vec),
            MessageValue::Array(vec) => {
                apache_avro::types::Value::Array(vec.into_iter().map(Into::into).collect())
            }
            MessageValue::Object(hash_map) => apache_avro::types::Value::Map(
                hash_map
                    .into_iter()
                    .map(|(key, value)| (key.into(), value.into()))
                    .collect(),
            ),
        }
    }
}
