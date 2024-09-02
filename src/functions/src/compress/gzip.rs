use std::io::{Read, Write};

use flate2::{read::GzDecoder, write::GzEncoder, Compression};

fn encode() {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(b"Example").unwrap();
    encoder.flush().unwrap();
}

fn decode() {
    // let mut decoder = GzDecoder::new();
    // decoder.read_to_end(buf);
}
