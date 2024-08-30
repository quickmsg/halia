use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use tokio::{select, sync::mpsc};
use types::{
    databoard::{CreateUpdateDataReq, DataConf, SearchDatasInfoResp},
    BaseConf,
};
use uuid::Uuid;

pub struct Data {
    pub id: Uuid,
    base_conf: BaseConf,
    ext_conf: DataConf,

    stop_signal_tx: mpsc::Sender<()>,
    pub mb_tx: mpsc::Sender<MessageBatch>,
}

impl Data {
    pub async fn new(id: Uuid, req: CreateUpdateDataReq) -> HaliaResult<Self> {
        Self::validate_conf(&req.ext)?;

        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        let (mb_tx, mb_rx) = mpsc::channel(16);

        let data = Data {
            id,
            base_conf: req.base,
            ext_conf: req.ext,
            mb_tx,
            stop_signal_tx,
        };

        Self::event_loop(stop_signal_rx, mb_rx).await;

        Ok(data)
    }

    fn validate_conf(_conf: &DataConf) -> HaliaResult<()> {
        Ok(())
    }

    pub fn check_duplicate(&self, req: &CreateUpdateDataReq) -> HaliaResult<()> {
        if self.base_conf.name == req.base.name {
            return Err(HaliaError::NameExists);
        }

        Ok(())
    }

    async fn event_loop(
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
    ) {
        loop {
            select! {
                _ = stop_signal_rx.recv() => {
                    return;
                }

                mb = mb_rx.recv() => {
                    if let Some(mb) = mb {
                        // todo
                    }
                }
            }
        }
    }

    pub fn search(&self) -> SearchDatasInfoResp {
        SearchDatasInfoResp {
            id: self.id.clone(),
            conf: CreateUpdateDataReq {
                base: self.base_conf.clone(),
                ext: self.ext_conf.clone(),
            },
        }
    }

    pub async fn update(&mut self, req: CreateUpdateDataReq) -> HaliaResult<()> {
        Self::validate_conf(&req.ext)?;

        let mut restart = false;
        if self.ext_conf != req.ext {
            restart = true;
        }
        self.base_conf = req.base;
        self.ext_conf = req.ext;

        if restart {
            self.stop_signal_tx.send(()).await.unwrap();

            // let (stop_signal_rx, publish_rx, tx, device_err) =
            //     self.join_handle.take().unwrap().await.unwrap();
            // self.event_loop(
            //     stop_signal_rx,
            //     publish_rx,
            //     tx,
            //     self.ext_conf.clone(),
            //     device_err,
            // )
            // .await;
            todo!()
        }

        Ok(())
    }

    pub async fn stop(&mut self) {
        self.stop_signal_tx.send(()).await.unwrap();
    }
}
