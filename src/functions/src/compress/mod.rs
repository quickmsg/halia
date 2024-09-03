use message::MessageBatch;

mod deflate;
mod gzip;
mod zlib;

pub(crate) trait Compresser {
    fn code(&mut self, mb: &mut MessageBatch);
}
