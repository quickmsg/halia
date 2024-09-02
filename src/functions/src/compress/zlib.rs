use std::io::{self, Write as _};

use flate2::{
    write::{ZlibDecoder, ZlibEncoder},
    Compression,
};

fn decode_reader(bytes: Vec<u8>) -> io::Result<String> {
    let mut writer = Vec::new();
    let mut z = ZlibDecoder::new(writer);
    z.write_all(&bytes[..])?;
    writer = z.finish()?;
    let return_string = String::from_utf8(writer).expect("String parsing error");
    Ok(return_string)
}

fn test() {
    let mut e = ZlibEncoder::new(Vec::new(), Compression::default());
    e.write_all(b"Hello World").unwrap();
    let compressed = e.finish().unwrap();
}
