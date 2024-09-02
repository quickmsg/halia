use std::io::{self, Write as _};

use bytes::{BufMut, Bytes};
use flate2::{write::DeflateDecoder, write::DeflateEncoder, Compression};

use super::Compresser;

struct HaliaDeflateEncoder {
    field: String,
    target_field: String,
    deflater: DeflateDecoder<Vec<u8>>,
}

pub fn new_encoder(field: String, target_field: String) -> Box<dyn Compresser> {
    let deflater = DeflateDecoder::new(vec![]);
    Box::new(HaliaDeflateEncoder {
        field,
        target_field,
        deflater,
    })
}

impl Compresser for HaliaDeflateEncoder {
    fn code(&self, mb: &mut message::MessageBatch) {
        for message in mb.get_messages_mut() {}

        todo!()
        // fn decode_writer(bytes: Vec<u8>) -> io::Result<String> {

        // deflater.write_all(&bytes[..])?;
        // writer = deflater.finish()?;
        // let return_string = String::from_utf8(writer).expect("String parsing error");
        // Ok(return_string)
        // }
    }
}

struct HaliaDeflateDecoder {}

impl Compresser for HaliaDeflateDecoder {
    fn code(&self, mb: &mut message::MessageBatch) {
        // let mut e = DeflateEncoder::new(Vec::new(), Compression::default());
        // e.write_all(b"Hello World").unwrap();
        // println!("{:?}", e.finish().unwrap());
        todo!()
    }
}
