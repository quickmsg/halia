pub mod avron;
pub mod csv;

pub enum Schema {
    Json,
    Csv,
    Avron,
}