use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
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
    apps::http_client::SinkConf,
    devices::{
        opcua::{OpcuaConf, SourceConf},
        DeviceConf, SearchDevicesItemRunningInfo,
    },
    Value,
};

use crate::Device;

mod sink;
mod source;

struct Opcua {
    id: String,

    err: Option<String>,
    stop_signal_tx: mpsc::Sender<()>,
    opcua_client: Arc<Session>,

    sources: DashMap<String, Source>,
    sinks: DashMap<String, Sink>,
}

pub fn new(id: String, device_conf: DeviceConf) -> HaliaResult<Box<dyn Device>> {
    let conf: OpcuaConf = serde_json::from_value(device_conf.ext)?;
    let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);

    let opcua = Opcua {
        id: id,
        err: None,
        opcua_client: todo!(),
        stop_signal_tx,
        sources: DashMap::new(),
        sinks: DashMap::new(),
    };

    todo!()
}

pub fn validate_conf(_conf: &serde_json::Value) -> HaliaResult<()> {
    Ok(())
}

pub fn validate_source_conf(_conf: &serde_json::Value) -> HaliaResult<()> {
    Ok(())
}
pub fn validate_sink_conf(_conf: &serde_json::Value) -> HaliaResult<()> {
    Ok(())
}

impl Opcua {
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

    async fn update(
        &mut self,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
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

    async fn create_source(
        &mut self,
        source_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SourceConf = serde_json::from_value(conf)?;
        let source = Source::new(self.opcua_client.clone(), conf).await;
        self.sources.insert(source_id, source);
        Ok(())
    }

    async fn update_source(
        &mut self,
        source_id: &String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let old_conf: SourceConf = serde_json::from_value(old_conf)?;
        let new_conf: SourceConf = serde_json::from_value(new_conf)?;
        match self.sources.get_mut(source_id) {
            Some(mut source) => {
                source.update_conf(old_conf, new_conf).await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(source_id.to_string())),
        }
    }

    async fn write_source_value(&mut self, source_id: String, value: Value) -> HaliaResult<()> {
        todo!()
    }

    async fn delete_source(&mut self, source_id: &String) -> HaliaResult<()> {
        match self.sources.remove(source_id) {
            Some((_, mut source)) => {
                source.stop().await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(source_id.to_string())),
        }
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf)?;
        let sink = Sink::new(self.opcua_client.clone(), conf).await?;
        self.sinks.insert(sink_id, sink);
        Ok(())
    }

    async fn update_sink(
        &mut self,
        sink_id: &String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let old_conf: SinkConf = serde_json::from_value(old_conf)?;
        let new_conf: SinkConf = serde_json::from_value(new_conf)?;
        match self.sinks.get_mut(sink_id) {
            Some(mut sink) => {
                sink.update_conf(old_conf, new_conf).await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(sink_id.to_string())),
        }
    }

    async fn delete_sink(&mut self, sink_id: &String) -> HaliaResult<()> {
        match self.sinks.remove(sink_id) {
            Some((_, mut sink)) => {
                sink.stop().await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(sink_id.to_string())),
        }
    }

    async fn get_source_rx(
        &self,
        source_id: &String,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self.sources.get(source_id) {
            Some(source) => Ok(source.mb_tx.subscribe()),
            None => Err(HaliaError::NotFound(source_id.to_string())),
        }
    }

    async fn get_sink_tx(&self, sink_id: &String) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.get(sink_id) {
            Some(sink) => Ok(sink.mb_tx.clone()),
            None => Err(HaliaError::NotFound(sink_id.to_string())),
        }
    }

    async fn stop(&mut self) {
        // todo 判断当前是否错误
        match self.opcua_client.disconnect().await {
            Ok(_) => {
                debug!("session disconnect success");
            }
            Err(e) => {
                debug!("err code is :{}", e);
            }
        }
    }
}
