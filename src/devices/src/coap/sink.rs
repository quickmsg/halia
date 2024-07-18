use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,
    items: Vec<SinkItem>,
    conf: Conf,
}

struct Conf {}

struct SinkItem {
    method: Method,
    path: String,
    queries: Option<String>,
    timeout: Option<usize>,
}

enum Method {
    POST,
    PUT,
    DELETE,
}
