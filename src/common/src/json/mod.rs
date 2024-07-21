pub fn get_u8(value: &serde_json::Value) -> Option<u8> {
    match value.as_u64() {
        Some(n) => {
            if n > u8::MAX as u64 {
                None
            } else {
                Some(n as u8)
            }
        }
        None => None,
    }
}

pub fn get_u16(value: &serde_json::Value) -> Option<u16> {
    match value.as_u64() {
        Some(n) => {
            if n > u16::MAX as u64 {
                None
            } else {
                Some(n as u16)
            }
        }
        None => None,
    }
}