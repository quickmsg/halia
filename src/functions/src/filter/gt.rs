use message::Message;

pub struct Gt {
    field: String,
    value: Value,
}

impl Gt {
    pub fn new() -> Self {
        Self {
            field: todo!(),
            value: todo!(),
        }
    }
}

impl Gt {
    fn filter(&self, msg: &Message) -> bool {
        match msg.get(&self.field) {
            Some(value) => match value {
                json::Value::Int8(number) => match self.value {
                    Value::Int(i64) => *number > i64 as i8,
                    Value::Float(f64) => *number > f64 as i8,
                    Value::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            json::Value::Int8(value) => number > value,
                            json::Value::Int16(_) => todo!(),
                            json::Value::Int32(_) => todo!(),
                            json::Value::Int64(_) => todo!(),
                            json::Value::UInt8(_) => todo!(),
                            json::Value::UInt16(_) => todo!(),
                            json::Value::UInt32(_) => todo!(),
                            json::Value::UInt64(_) => todo!(),
                            json::Value::Float32(_) => todo!(),
                            json::Value::Float64(_) => todo!(),
                            _ => false,
                        },
                        None => false,
                    },
                },
                json::Value::Int16(number) => *number as i64 > self.value,
                json::Value::Int32(number) => *number as i64 > self.value,
                json::Value::Int64(number) => *number as i64 > self.value,
                json::Value::UInt8(number) => *number as i64 > self.value,
                json::Value::UInt16(number) => *number as i64 > self.value,
                json::Value::UInt32(number) => *number as i64 > self.value,
                json::Value::UInt64(number) => *number as i64 > self.value,
                json::Value::Float32(number) => *number > self.value as f32,
                json::Value::Float64(number) => *number > self.value as f64,
                _ => false,
            },
            None => false,
        }
    }
}

enum Value {
    Int(i64),
    Float(f64),
    Field(String),
}