pub mod error;
pub mod persistence;

pub fn check_page_size(i: usize, page: usize, size: usize) -> bool {
    i >= (page - 1) * size && i < page * size
}
