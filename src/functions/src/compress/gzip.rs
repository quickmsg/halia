use std::io::{Read, Write};

use flate2::{read::GzDecoder, write::GzEncoder, Compress, Compression};

fn test() {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(b"Example").unwrap();
    encoder.flush().unwrap();
}

fn test_2() {
    // let mut decoder = GzDecoder::new();
    // decoder.read_to_end(buf);
}
