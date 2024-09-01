pub mod avro;
pub mod csv;

pub enum Schema {
    Json,
    Csv,
    Avro,
}