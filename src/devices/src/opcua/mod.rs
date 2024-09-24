use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use common::{
    error::{HaliaError, HaliaResult},
    ref_info::RefInfo,
};
use message::MessageBatch;
use opcua::{
    client::{ClientBuilder, IdentityToken, Session},
    types::{EndpointDescription, StatusCode},
};
use sink::Sink;
use source::Source;
use tokio::{
    sync::{broadcast, mpsc, RwLock},
    task::JoinHandle,
};
use tracing::debug;
use types::{
    devices::{
        opcua::{OpcuaConf, SourceConf},
        CreateUpdateDeviceReq, DeviceConf, DeviceType, QueryParams, SearchDevicesItemCommon,
        SearchDevicesItemConf, SearchDevicesItemResp, SearchDevicesItemRunningInfo,
    },
    BaseConf, CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksInfoResp,
    SearchSourcesOrSinksItemResp, SearchSourcesOrSinksResp, Value,
};

use crate::Device;

mod sink;
mod source;

struct Opcua {
    id: String,
    base_conf: BaseConf,
    ext_conf: OpcuaConf,

    on: bool,
    err: Option<String>,
    stop_signal_tx: Option<mpsc::Sender<()>>,
    opcua_client: Arc<RwLock<Option<Arc<Session>>>>,

    sources: Vec<Source>,
    source_ref_infos: Vec<(String, RefInfo)>,
    sinks: Vec<Sink>,
    sink_ref_infos: Vec<(String, RefInfo)>,
}

pub async fn new(id: String, device_conf: DeviceConf) -> HaliaResult<Box<dyn Device>> {
    let ext_conf: OpcuaConf = serde_json::from_value(device_conf.ext)?;
    Opcua::validate_conf(&ext_conf)?;

    Ok(Box::new(Opcua {
        id: id,
        base_conf: device_conf.base,
        ext_conf,
        on: false,
        err: None,
        opcua_client: Arc::new(RwLock::new(None)),
        stop_signal_tx: None,
        sources: vec![],
        source_ref_infos: vec![],
        sinks: vec![],
        sink_ref_infos: vec![],
    }))
}

impl Opcua {
    fn validate_conf(_conf: &OpcuaConf) -> HaliaResult<()> {
        Ok(())
    }

    async fn connect(
        opcua_conf: &OpcuaConf,
    ) -> HaliaResult<(Arc<Session>, JoinHandle<StatusCode>)> {
        let mut client = ClientBuilder::new()
            .application_name("test")
            .application_uri("aasda")
            .trust_server_certs(true)
            .session_retry_limit(3)
            .create_sample_keypair(true)
            .keep_alive_interval(Duration::from_millis(100))
            .client()
            .unwrap();

        let endpoint: EndpointDescription = EndpointDescription::from(opcua_conf.host.as_ref());

        let (session, event_loop) = match client
            .new_session_from_endpoint(endpoint, IdentityToken::Anonymous)
            .await
        {
            Ok((session, event_loop)) => (session, event_loop),
            Err(e) => {
                debug!("{:?}", e);
                return Err(HaliaError::Common(e.to_string()));
            }
        };

        let handle = event_loop.spawn();
        session.wait_for_connection().await;
        Ok((session, handle))
    }

    // async fn start(&mut self) -> HaliaResult<()> {
    //     check_and_set_on_true!(self);

    //     persistence::update_device_status(&self.id, Status::Runing).await?;

    //     let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
    //     self.stop_signal_tx = Some(stop_signal_tx);

    //     self.event_loop(stop_signal_rx).await;
    //     Ok(())
    // }

    // async fn event_loop(&mut self, mut stop_signal_rx: mpsc::Receiver<()>) {
    //     let opcua_conf = self.conf.ext.clone();
    //     let global_session = self.session.clone();
    //     let reconnect = self.conf.ext.reconnect;
    //     let groups = self.groups.clone();
    //     tokio::spawn(async move {
    //         loop {
    //             match Opcua::connect(&opcua_conf).await {
    //                 Ok((session, join_handle)) => {
    //                     for group in groups.write().await.iter_mut() {
    //                         group.start(session.clone()).await;
    //                     }

    //                     *(global_session.write().await) = Some(session);
    //                     match join_handle.await {
    //                         Ok(s) => {
    //                             debug!("{}", s);
    //                         }
    //                         Err(e) => debug!("{}", e),
    //                     }
    //                 }
    //                 Err(e) => {
    //                     let sleep = time::sleep(Duration::from_secs(reconnect));
    //                     tokio::pin!(sleep);
    //                     select! {
    //                         _ = stop_signal_rx.recv() => {
    //                             return
    //                         }

    //                         _ = &mut sleep => {}
    //                     }
    //                     debug!("{e}");
    //                 }
    //             }
    //         }
    //     });
    // }

    async fn stop(&mut self) -> HaliaResult<()> {
        // for group in self.groups.write().await.iter_mut() {
        //     group.stop().await?;
        // }

        match self
            .opcua_client
            .read()
            .await
            .as_ref()
            .unwrap()
            .disconnect()
            .await
        {
            Ok(_) => {
                debug!("session disconnect success");
            }
            Err(e) => {
                debug!("err code is :{}", e);
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Device for Opcua {
    async fn read_running_info(&self) -> SearchDevicesItemRunningInfo {
        todo!()
        // let err = self.err.read().await.clone();
        // let rtt = match (self.on, &self.err) {
        //     (true, None) => Some(999),
        //     _ => None,
        // };
        // SearchDevicesItemResp {
        //     common: SearchDevicesItemCommon {
        //         id: self.id.clone(),
        //         device_type: DeviceType::Opcua,
        //         on: self.on,
        //         err: self.err.clone(),
        //         rtt,
        //     },
        //     conf: SearchDevicesItemConf {
        //         base: self.base_conf.clone(),
        //         ext: serde_json::json!(self.ext_conf),
        //     },
        //     source_cnt: self.source_ref_infos.len(),
        //     sink_cnt: self.sink_ref_infos.len(),
        // }
    }

    async fn update(&mut self, old_conf: String, new_conf: &serde_json::Value) -> HaliaResult<()> {
        // let ext_conf: OpcuaConf = serde_json::from_value(device_conf.ext)?;
        // Self::validate_conf(&ext_conf)?;

        // let mut restart = false;
        // if self.ext_conf != ext_conf {
        //     restart = true;
        // }
        // self.base_conf = device_conf.base;
        // self.ext_conf = ext_conf;

        // if restart && self.on {
        //     self.stop_signal_tx
        //         .as_ref()
        //         .unwrap()
        //         .send(())
        //         .await
        //         .unwrap();
        // }

        // Ok(())
        todo!()
    }

    // async fn delete(&mut self) -> HaliaResult<()> {
    //     check_delete_all!(self, source);
    //     check_delete_all!(self, sink);

    //     if self.on {
    //         self.stop().await?;
    //     }

    //     Ok(())
    // }

    async fn create_source(
        &mut self,
        source_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        // let ext_conf: SourceConf = serde_json::from_value(req.ext)?;
        // for source in self.sources.iter() {
        //     source.check_duplicate(&req.base, &ext_conf)?;
        // }

        // let mut source = Source::new(source_id, req.base, ext_conf)?;
        // if self.on {
        //     // source.start(self.opcua_client.as_ref().unwarp());
        // }

        // Ok(())

        todo!()
    }

    async fn update_source(
        &mut self,
        source_id: &String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        todo!()
    }

    async fn delete_source(&mut self, source_id: &String) -> HaliaResult<()> {
        todo!()
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
        todo!()
        // match Sink::new(sink_id, req).await {
        //     Ok(sink) => {
        //         if self.on {
        //             //
        //         }
        //         self.sinks.push(sink);
        //         Ok(())
        //     }
        //     Err(e) => Err(e),
        // }
    }

    async fn update_sink(
        &mut self,
        sink_id: &String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        todo!()
    }

    async fn delete_sink(&mut self, sink_id: &String) -> HaliaResult<()> {
        todo!()
    }

    async fn get_source_rx(
        &self,
        source_id: &String,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        // self.sources
        //     .get(source_id)
        //     .ok_or(HaliaError::NotFound)?
        //     .mb_tx
        //     .as_ref()
        //     .subscribe()
        // match self
        //     .sources
        //     .iter_mut()
        //     .find(|source| source.id == *source_id)
        // {
        //     Some(source) => Ok(source.mb_tx.as_ref().unwrap().subscribe()),
        //     None => unreachable!(),
        // }
        todo!()
    }

    async fn get_sink_tx(&self, sink_id: &String) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.iter().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.mb_tx.as_ref().unwrap().clone()),
            None => unreachable!(),
        }
    }

    // async fn start(&mut self) -> HaliaResult<()> {
    //     check_and_set_on_true!(self);
    //     add_device_on_count();

    //     let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
    //     self.stop_signal_tx = Some(stop_signal_tx);

    //     // self.event_loop(stop_signal_rx).await;
    //     Ok(())
    // }

    async fn stop(&mut self) -> HaliaResult<()> {
        todo!()
    }
    async fn write_source_value(&mut self, source_id: String, req: Value) -> HaliaResult<()> {
        todo!()
    }
}
