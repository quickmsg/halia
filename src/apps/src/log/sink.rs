use common::{
    error::{HaliaError, HaliaResult},
    get_search_sources_or_sinks_info_resp,
};
use message::MessageBatch;
use tokio::{select, sync::mpsc, task::JoinHandle};
use tracing::debug;
use types::{
    apps::log::SinkConf, BaseConf, CreateUpdateSourceOrSinkReq, SearchSourcesOrSinksInfoResp,
};
use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,

    base_conf: BaseConf,
    ext_conf: SinkConf,

    stop_signal_tx: Option<mpsc::Sender<()>>,
    join_handle: Option<JoinHandle<(mpsc::Receiver<()>, mpsc::Receiver<MessageBatch>)>>,

    pub mb_tx: Option<mpsc::Sender<MessageBatch>>,
}

impl Sink {
    pub fn new(sink_id: Uuid, base_conf: BaseConf, ext_conf: SinkConf) -> Self {
        Self {
            id: sink_id,
            base_conf,
            ext_conf,
            stop_signal_tx: None,
            join_handle: None,
            mb_tx: None,
        }
    }

    pub fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub fn check_duplicate(&self, base_conf: &BaseConf, _ext_conf: &SinkConf) -> HaliaResult<()> {
        if self.base_conf.name == base_conf.name {
            return Err(HaliaError::NameExists);
        }

        Ok(())
    }

    pub fn search(&self) -> SearchSourcesOrSinksInfoResp {
        get_search_sources_or_sinks_info_resp!(self, None)
    }

    pub async fn update(&mut self, base_conf: BaseConf, _ext_conf: SinkConf) {
        self.base_conf = base_conf;

        match &self.stop_signal_tx {
            Some(stop_signal_tx) => {
                stop_signal_tx.send(()).await.unwrap();

                let (stop_signal_rx, mb_rx) = self.join_handle.take().unwrap().await.unwrap();
                self.event_loop(stop_signal_rx, mb_rx);
            }
            None => {}
        }
    }

    pub fn start(&mut self) {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (mb_tx, mb_rx) = mpsc::channel(16);
        self.mb_tx = Some(mb_tx);

        self.event_loop(stop_signal_rx, mb_rx);
    }

    fn event_loop(
        &mut self,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
    ) {
        let name = self.base_conf.name.clone();
        let join_handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, mb_rx);
                    }

                    mb = mb_rx.recv() => {
                        debug!("{} received {:?}", name, mb);
                    }
                }
            }
        });
        self.join_handle = Some(join_handle);
    }

    pub async fn stop(&mut self) {
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
        self.stop_signal_tx = None;
    }
}
