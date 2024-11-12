pub struct S7Conf {
    pub cpu_type: CpuType,
    pub host: String,
    // default: 102
    pub port: u16,

    pub rack: u16,
    pub slot: u16
}

pub enum CpuType {
    S200,
    S300,
    S400,
    S1200,
    S1500,
}