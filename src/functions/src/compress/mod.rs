use message::MessageBatch;

mod deflate;
mod gzip;
mod zlib;
mod snappy;
mod brotli;
mod lz4;

pub(crate) trait Compresser {
    fn code(&mut self, mb: &mut MessageBatch);
}