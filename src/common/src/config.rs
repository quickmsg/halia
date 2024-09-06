use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub port: u16,
    pub storage: Option<Storage>,
}

#[derive(Deserialize)]
pub enum Storage {
    Sqlite(Sqlite),
    Mysql(Mysql),
    Postgresql(Postgresql),
}

#[derive(Deserialize)]
pub struct Sqlite {

}

#[derive(Deserialize)]
pub struct Mysql {

}

#[derive(Deserialize)]
pub struct Postgresql {

}