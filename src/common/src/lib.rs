#![feature(pattern)]

use uuid::Uuid;

pub mod error;
pub mod json;
pub mod macros;
pub mod persistence;
pub mod ref_info;

pub fn check_page_size(i: usize, page: usize, size: usize) -> bool {
    i >= (page - 1) * size && i < page * size
}

pub fn get_id(id: Option<Uuid>) -> (Uuid, bool) {
    match id {
        Some(id) => (id, false),
        None => (Uuid::new_v4(), true),
    }
}
