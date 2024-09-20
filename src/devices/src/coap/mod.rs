use std::{collections::HashSet, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use base64::{prelude::BASE64_STANDARD, Engine as _};
use common::{
    error::{HaliaError, HaliaResult},
    ref_info::RefInfo,
};
use message::MessageBatch;
use protocol::coap::{client::UdpCoAPClient, request::CoapOption};
use rand::{rngs::StdRng, Rng, SeedableRng};
use sink::Sink;
use source::Source;
use tokio::sync::{broadcast, mpsc, Mutex};
use types::{
    devices::{coap::CoapConf, DeviceConf, SearchDevicesItemRunningInfo},
    BaseConf, Value,
};

use crate::Device;

mod sink;
mod source;

macro_rules! coap_not_support_write_source_value {
    () => {
        Err(HaliaError::Common("coap设备不支持写入源数据!".to_owned()))
    };
}

struct Coap {
    id: String,
    base_conf: BaseConf,
    ext_conf: CoapConf,

    sources: Vec<Source>,
    source_ref_infos: Vec<(String, RefInfo)>,
    sinks: Vec<Sink>,
    sink_ref_infos: Vec<(String, RefInfo)>,

    coap_client: Option<Arc<UdpCoAPClient>>,
    err: Option<String>,
    token_manager: Arc<Mutex<TokenManager>>,
}

pub async fn new(id: String, device_conf: DeviceConf) -> HaliaResult<Box<dyn Device>> {
    let ext_conf: CoapConf = serde_json::from_value(device_conf.ext)?;
    Coap::validate_conf(&ext_conf)?;

    Ok(Box::new(Coap {
        id,
        base_conf: device_conf.base,
        ext_conf,
        sources: vec![],
        source_ref_infos: vec![],
        sinks: vec![],
        sink_ref_infos: vec![],
        coap_client: None,
        err: None,
        token_manager: Arc::new(Mutex::new(TokenManager::new())),
    }))
}

impl Coap {
    fn validate_conf(_conf: &CoapConf) -> HaliaResult<()> {
        Ok(())
    }
}

#[async_trait]
impl Device for Coap {
    async fn read_running_info(&self) -> SearchDevicesItemRunningInfo {
        todo!()
        // SearchDevicesItemResp {
        //     common: SearchDevicesItemCommon {
        //         id: self.id.clone(),
        //         device_type: DeviceType::Coap,
        //         rtt: None,
        //         on: self.on,
        //         err: self.err.clone(),
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
        todo!()
        // let ext_conf: CoapConf = serde_json::from_value(device_conf.ext)?;
        // Self::validate_conf(&ext_conf)?;

        // let mut restart = false;
        // if self.ext_conf != ext_conf {
        //     restart = true;
        // }
        // self.base_conf = device_conf.base;
        // self.ext_conf = ext_conf;

        // if self.on && restart {
        //     let coap_client = Arc::new(
        //         UdpCoAPClient::new_udp((self.ext_conf.host.clone(), self.ext_conf.port)).await?,
        //     );
        //     for source in self.sources.iter_mut() {
        //         _ = source.update_coap_client(coap_client.clone());
        //     }
        // for sink in self.sinks.iter_mut() {
        //     _ = sink.stop();
        // }

        // let coap_client = Arc::new(
        //     UdpCoAPClient::new_udp((self.ext_conf.host.clone(), self.ext_conf.port)).await?,
        // );
        // for source in self.sources.iter_mut() {
        //     _ = source.start(coap_client.clone());
        // }
        // for sink in self.sinks.iter_mut() {
        //     _ = sink.start(coap_client.clone());
        // }
        //     self.coap_client = Some(coap_client);
        // }

        // Ok(())
    }

    // async fn start(&mut self) -> HaliaResult<()> {
    //     check_and_set_on_true!(self);
    //     add_device_on_count();

    //     let client = Arc::new(
    //         UdpCoAPClient::new_udp((self.ext_conf.host.clone(), self.ext_conf.port)).await?,
    //     );

    //     for source in self.sources.iter_mut() {
    //         _ = source.start(client.clone()).await;
    //     }

    //     for sink in self.sinks.iter_mut() {
    //         _ = sink.start(client.clone()).await;
    //     }

    //     Ok(())
    // }

    async fn stop(&mut self) -> HaliaResult<()> {
        for source in self.sources.iter_mut() {
            _ = source.stop().await;
        }
        for sink in self.sinks.iter_mut() {
            _ = sink.stop().await;
        }

        Ok(())
    }

    // async fn delete(&mut self) -> HaliaResult<()> {
    //     debug!("here");
    //     // check_delete!(self, sources_ref_infos);
    //     // check_delete!(self, sinks_ref_infos);

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
        // let mut source = Source::new(source_id, req.base, ext_conf, self.token_manager.clone())?;
        // if self.on {
        //     _ = source
        //         .start(self.coap_client.as_ref().unwrap().clone())
        //         .await;
        // }

        // self.sources.push(source);
        // self.source_ref_infos.push((source_id, RefInfo::new()));
        // Ok(())
        todo!()
    }

    async fn update_source(
        &mut self,
        source_id: &String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        // let ext_conf: SourceConf = serde_json::from_value(req.ext)?;
        // for source in self.sources.iter() {
        //     if source.id != source_id {
        //         source.check_duplicate(&req.base, &ext_conf)?;
        //     }
        // }

        // match self
        //     .sources
        //     .iter_mut()
        //     .find(|source| source.id == source_id)
        // {
        //     Some(source) => source.update_conf(req.base, ext_conf).await,
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    async fn write_source_value(&mut self, _source_id: String, _req: Value) -> HaliaResult<()> {
        coap_not_support_write_source_value!()
    }

    async fn delete_source(&mut self, source_id: &String) -> HaliaResult<()> {
        // match self
        //     .sources
        //     .iter_mut()
        //     .find(|source| source.id == source_id)
        // {
        //     Some(source) => source.stop().await,
        //     None => unreachable!(),
        // }

        // self.sources.retain(|source| source.id != source_id);
        // self.source_ref_infos.retain(|(id, _)| *id != source_id);
        // Ok(())
        todo!()
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
        todo!()
        // let ext_conf: SinkConf = serde_json::from_value(req.ext)?;

        // for sink in self.sinks.iter() {
        //     sink.check_duplicate(&req.base, &ext_conf)?;
        // }

        // let mut sink = Sink::new(sink_id, req.base, ext_conf, self.token_manager.clone())?;
        // if self.on {
        //     _ = sink.start(self.coap_client.as_ref().unwrap().clone()).await;
        // }

        // self.sinks.push(sink);
        // self.sink_ref_infos.push((sink_id, RefInfo::new()));

        // Ok(())
    }

    async fn update_sink(
        &mut self,
        sink_id: &String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        todo!()
        //     let ext_conf: SinkConf = serde_json::from_value(req.ext)?;

        //     for sink in self.sinks.iter() {
        //         if sink.id != sink_id {
        //             sink.check_duplicate(&req.base, &ext_conf)?;
        //         }
        //     }

        //     match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
        //         Some(sink) => sink.update(req.base, ext_conf).await,
        //         None => Err(HaliaError::NotFound),
        //     }
    }

    async fn delete_sink(&mut self, sink_id: &String) -> HaliaResult<()> {
        // match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
        //     Some(sink) => sink.stop().await,
        //     None => unreachable!(),
        // }

        // self.sinks.retain(|sink| sink.id != sink_id);
        // self.sink_ref_infos.retain(|(id, _)| *id != sink_id);
        Ok(())
    }

    async fn get_source_rx(
        &mut self,
        source_id: &String,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
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

    async fn get_sink_tx(&mut self, sink_id: &String) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        // match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
        //     Some(sink) => Ok(sink.mb_tx.as_ref().unwrap().clone()),
        //     None => unreachable!(),
        // }
        todo!()
    }
}

pub(crate) fn transform_options(
    input_options: &Vec<(types::devices::coap::CoapOption, String)>,
) -> Result<Vec<(CoapOption, Vec<u8>)>> {
    let mut options = vec![];
    for (k, v) in input_options {
        let v = BASE64_STANDARD.decode(&v)?;
        match k {
            types::devices::coap::CoapOption::IfMatch => options.push((CoapOption::IfMatch, v)),
            types::devices::coap::CoapOption::UriHost => options.push((CoapOption::UriHost, v)),
            types::devices::coap::CoapOption::ETag => options.push((CoapOption::ETag, v)),
            types::devices::coap::CoapOption::IfNoneMatch => {
                options.push((CoapOption::IfNoneMatch, v))
            }
            types::devices::coap::CoapOption::Observe => options.push((CoapOption::Observe, v)),
            types::devices::coap::CoapOption::UriPort => options.push((CoapOption::UriPort, v)),
            types::devices::coap::CoapOption::LocationPath => {
                options.push((CoapOption::LocationPath, v))
            }
            types::devices::coap::CoapOption::Oscore => options.push((CoapOption::Oscore, v)),
            types::devices::coap::CoapOption::UriPath => options.push((CoapOption::UriPath, v)),
            types::devices::coap::CoapOption::ContentFormat => {
                options.push((CoapOption::ContentFormat, v))
            }
            types::devices::coap::CoapOption::MaxAge => options.push((CoapOption::MaxAge, v)),
            types::devices::coap::CoapOption::UriQuery => options.push((CoapOption::UriQuery, v)),
            types::devices::coap::CoapOption::Accept => options.push((CoapOption::Accept, v)),
            types::devices::coap::CoapOption::LocationQuery => {
                options.push((CoapOption::LocationQuery, v))
            }
            types::devices::coap::CoapOption::Block2 => options.push((CoapOption::Block2, v)),
            types::devices::coap::CoapOption::Block1 => options.push((CoapOption::Block1, v)),
            types::devices::coap::CoapOption::ProxyUri => options.push((CoapOption::ProxyUri, v)),
            types::devices::coap::CoapOption::ProxyScheme => {
                options.push((CoapOption::ProxyScheme, v))
            }
            types::devices::coap::CoapOption::Size1 => options.push((CoapOption::Size1, v)),
            types::devices::coap::CoapOption::Size2 => options.push((CoapOption::Size2, v)),
            types::devices::coap::CoapOption::NoResponse => {
                options.push((CoapOption::NoResponse, v))
            }
        }
    }
    Ok(options)
}

pub struct TokenManager {
    // 0-8个字节
    tokens: HashSet<Vec<u8>>,
    rng: StdRng,
}

impl TokenManager {
    pub fn new() -> Self {
        Self {
            tokens: HashSet::new(),
            rng: StdRng::from_entropy(),
        }
    }

    pub fn acquire(&mut self) -> Vec<u8> {
        let mut token: Vec<u8> = vec![0; 8];
        loop {
            self.rng.fill(&mut token[..]);
            if !self.tokens.contains(&token) {
                self.tokens.insert(token.clone());
                return token;
            }
        }
    }

    pub fn release(&mut self, token: Vec<u8>) {
        self.tokens.remove(&token);
    }
}
