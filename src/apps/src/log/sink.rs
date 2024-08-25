use common::{error::HaliaResult, get_search_sources_or_sinks_info_resp, ref_info::RefInfo};
use message::MessageBatch;
use tokio::{select, sync::mpsc};
use tracing::debug;
use types::{
    apps::log::SinkConf, BaseConf, CreateUpdateSourceOrSinkReq, SearchSourcesOrSinksInfoResp,
};
use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,

    base_conf: BaseConf,
    ext_conf: SinkConf,

    pub stop_signal_tx: Option<mpsc::Sender<()>>,

    pub ref_info: RefInfo,
    pub mb_tx: Option<mpsc::Sender<MessageBatch>>,
}

impl Sink {
    pub fn new(sink_id: Uuid, req: CreateUpdateSourceOrSinkReq) -> HaliaResult<Self> {
        let ext_conf: SinkConf = serde_json::from_value(req.ext)?;

        Ok(Self {
            id: sink_id,
            base_conf: req.base,
            ext_conf,
            stop_signal_tx: None,
            ref_info: RefInfo::new(),
            mb_tx: None,
        })
    }

    pub fn search(&self) -> SearchSourcesOrSinksInfoResp {
        get_search_sources_or_sinks_info_resp!(self, None)
    }

    pub fn update(&mut self, req: CreateUpdateSourceOrSinkReq) -> HaliaResult<()> {
        todo!()
        // let mut restart = false;
        // if self.ext_conf != ext_conf {
        //     restart = true;
        // }
        // self.base_conf = base_conf;
        // self.ext_conf = ext_conf;

        // if self.on && restart {}
    }

    pub fn delete(&mut self) -> HaliaResult<()> {
        Ok(())
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
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return;
                    }

                    mb = mb_rx.recv() => {
                        debug!("{:?}", mb);
                    }
                }
            }
        });
    }

    pub fn stop(&mut self) {}

    pub fn get_tx(&mut self, rule_id: &Uuid) -> mpsc::Sender<MessageBatch> {
        self.ref_info.active_ref(rule_id);
        self.mb_tx.as_ref().unwrap().clone()
    }
}
