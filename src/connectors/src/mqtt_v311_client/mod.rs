mod sink;
mod source;

pub(crate) struct MqttV311 {
    // 连接超时时长
    timeout: usize,

    keep_alive: usize,
    clean_session: bool,
    ip: String,
    port: u16,
}

enum Conf {
    Password(PasswordConf),
}

struct PasswordConf {
    username: String,
    password: String,
}

pub fn new() {

}