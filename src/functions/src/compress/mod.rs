use message::MessageBatch;

mod deflate;
mod gzip;
mod zlib;

pub(crate) trait Compresser {
    fn code(&self, mb: &mut MessageBatch);
}
