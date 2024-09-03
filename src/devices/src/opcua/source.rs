use std::{sync::Arc, time::Duration};

use common::{
    error::{HaliaError, HaliaResult},
    get_search_sources_or_sinks_info_resp,
};
use message::MessageBatch;
use opcua::types::ReadValueId;
use tokio::{
    select,
    sync::{broadcast, mpsc, RwLock},
    task::JoinHandle,
    time,
};
use types::{
    devices::opcua::SourceConf, BaseConf, CreateUpdateSourceOrSinkReq, SearchSourcesOrSinksInfoResp,
};
use uuid::Uuid;

use super::variable::Variable;

pub struct Source {
    pub id: Uuid,

    pub base_conf: BaseConf,
    pub ext_conf: SourceConf,

    variables: Option<Arc<RwLock<(Vec<Variable>, Vec<ReadValueId>)>>>,

    stop_signal_tx: Option<mpsc::Sender<()>>,
    join_handle: Option<
        JoinHandle<(
            mpsc::Receiver<()>,
            mpsc::Sender<Uuid>,
            Arc<RwLock<Option<String>>>,
        )>,
    >,
    err: Option<String>,

    pub mb_tx: Option<broadcast::Sender<MessageBatch>>,
}

impl Source {
    pub fn new(source_id: Uuid, base_conf: BaseConf, ext_conf: SourceConf) -> HaliaResult<Self> {
        Self::validate_conf(&ext_conf)?;

        Ok(Self {
            id: source_id,
            base_conf,
            ext_conf,
            variables: None,
            stop_signal_tx: None,
            mb_tx: None,
            join_handle: None,
            err: None,
        })
    }

    fn validate_conf(_ext_conf: &SourceConf) -> HaliaResult<()> {
        Ok(())
    }

    pub fn check_duplicate(&self, base_conf: &BaseConf, _ext_conf: &SourceConf) -> HaliaResult<()> {
        if self.base_conf.name == base_conf.name {
            return Err(HaliaError::NameExists);
        }

        Ok(())
    }

    pub fn search(&self) -> SearchSourcesOrSinksInfoResp {
        get_search_sources_or_sinks_info_resp!(self)
    }

    pub async fn start(
        &mut self,
        read_tx: mpsc::Sender<Uuid>,
        device_err: Arc<RwLock<Option<String>>>,
    ) {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (mb_tx, _) = broadcast::channel(16);
        self.mb_tx = Some(mb_tx);

        // self.event_loop(self.ext_conf.interval, stop_signal_rx, read_tx, device_err)
        //     .await;
    }

    async fn event_loop(
        &mut self,
        interval: u64,
        mut stop_signal_rx: mpsc::Receiver<()>,
        read_tx: mpsc::Sender<Uuid>,
        device_err: Arc<RwLock<Option<String>>>,
    ) {
        let point_id = self.id.clone();
        let join_handle = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));
            loop {
                select! {
                    biased;
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, read_tx, device_err);
                    }

                    _ = interval.tick() => {
                        if device_err.read().await.is_none() {
                            _ = read_tx.send(point_id).await;
                        }
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

    pub async fn update(&mut self, base_conf: BaseConf, ext_conf: SourceConf) -> HaliaResult<()> {
        Self::validate_conf(&ext_conf)?;

        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = base_conf;
        self.ext_conf = ext_conf;

        if self.stop_signal_tx.is_some() && restart {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();

            let (stop_signal_rx, read_tx, device_err) =
                self.join_handle.take().unwrap().await.unwrap();
            // self.event_loop(self.ext_conf.interval, stop_signal_rx, read_tx, device_err)
            //     .await;
        }

        Ok(())
    }
}
