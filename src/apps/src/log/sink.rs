use common::ref_info::RefInfo;
use message::MessageBatch;
use tokio::sync::mpsc;
use types::{apps::log::SinkConf, BaseConf};
use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,

    base_conf: BaseConf,
    ext_conf: SinkConf,

    pub stop_signal_tx: Option<mpsc::Sender<()>>,

    pub ref_info: RefInfo,
    pub mb_tx: Option<mpsc::Sender<MessageBatch>>,
}
