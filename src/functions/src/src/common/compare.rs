use types::message::FieldValue;

pub fn eq(val1: &FieldValue, val2: &FieldValue) -> bool {
    match (val1, val2) {
        (FieldValue::Bool(bool1), FieldValue::Bool(bool2)) => bool1 == bool2,
        (FieldValue::Integer(int1), FieldValue::Integer(int2)) => int1 == int2,
        (FieldValue::Float(float1), FieldValue::Float(float2)) => float1 == float2,
        (FieldValue::String(string1), FieldValue::String(string2)) => string1 == string2,
        _ => false,
    }
}

pub fn neq(val1: &FieldValue, val2: &FieldValue) -> bool {
    match (val1, val2) {
        (FieldValue::Bool(bool1), FieldValue::Bool(bool2)) => bool1 != bool2,
        (FieldValue::Integer(int1), FieldValue::Integer(int2)) => int1 != int2,
        (FieldValue::Float(float1), FieldValue::Float(float2)) => float1 != float2,
        (FieldValue::String(string1), FieldValue::String(string2)) => string1 != string2,
        _ => false,
    }
}

pub fn gt(val1: &FieldValue, val2: &FieldValue) -> bool {
    match (val1, val2) {
        (FieldValue::Integer(int1), FieldValue::Integer(int2)) => int1 > int2,
        (FieldValue::Float(float1), FieldValue::Float(float2)) => float1 > float2,
        _ => false,
    }
}

pub fn gte(val1: &FieldValue, val2: &FieldValue) -> bool {
    match (val1, val2) {
        (FieldValue::Integer(int1), FieldValue::Integer(int2)) => int1 >= int2,
        (FieldValue::Float(float1), FieldValue::Float(float2)) => float1 >= float2,
        _ => false,
    }
}

pub fn lt(val1: &FieldValue, val2: &FieldValue) -> bool {
    match (val1, val2) {
        (FieldValue::Integer(int1), FieldValue::Integer(int2)) => int1 < int2,
        (FieldValue::Float(float1), FieldValue::Float(float2)) => float1 < float2,
        _ => false,
    }
}

pub fn lte(val1: &FieldValue, val2: &FieldValue) -> bool {
    match (val1, val2) {
        (FieldValue::Integer(int1), FieldValue::Integer(int2)) => int1 <= int2,
        (FieldValue::Float(float1), FieldValue::Float(float2)) => float1 <= float2,
        _ => false,
    }
}

//gt  gte in nin lt lte ne
