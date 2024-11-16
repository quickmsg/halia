use std::{collections::HashSet, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use base64::{prelude::BASE64_STANDARD, Engine as _};
use coap_protocol::{client::UdpCoAPClient, request::CoapOption};
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::RuleMessageBatch;
use rand::{rngs::StdRng, Rng, SeedableRng};
use sink::Sink;
use source::Source;
use tokio::sync::{mpsc, Mutex};
use types::{
    devices::device::coap::{CoapConf, SinkConf, SourceConf},
    Value,
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
    sources: DashMap<String, Source>,
    sinks: DashMap<String, Sink>,

    coap_client: Arc<UdpCoAPClient>,
    err: Option<String>,
    rtt: u16,
    token_manager: Arc<Mutex<TokenManager>>,
}

pub async fn new(id: String, conf: serde_json::Value) -> HaliaResult<Box<dyn Device>> {
    let conf: CoapConf = serde_json::from_value(conf)?;
    let coap_client = Arc::new(UdpCoAPClient::new_udp((conf.host.clone(), conf.port)).await?);

    Ok(Box::new(Coap {
        id,
        sources: DashMap::new(),
        sinks: DashMap::new(),
        coap_client,
        err: None,
        rtt: 0,
        token_manager: Arc::new(Mutex::new(TokenManager::new())),
    }))
}

pub fn validate_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let _conf: CoapConf = serde_json::from_value(conf.clone())?;
    Ok(())
}

pub fn validate_source_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: SourceConf = serde_json::from_value(conf.clone())?;
    Source::validate_conf(conf)
}

pub fn validate_sink_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: SinkConf = serde_json::from_value(conf.clone())?;
    Sink::validate_conf(conf)
}

#[async_trait]
impl Device for Coap {
    async fn read_err(&self) -> Option<String> {
        self.err.clone()
    }

    async fn update_customize_conf(&mut self, conf: serde_json::Value) -> HaliaResult<()> {
        // let _old_conf: CoapConf = serde_json::from_value(old_conf)?;
        // let new_conf: CoapConf = serde_json::from_value(new_conf)?;
        // let coap_client =
        //     Arc::new(UdpCoAPClient::new_udp((new_conf.host.clone(), new_conf.port)).await?);
        // for mut source in self.sources.iter_mut() {
        //     _ = source.update_coap_client(coap_client.clone());
        // }
        // for mut sink in self.sinks.iter_mut() {
        //     _ = sink.update_coap_client(coap_client.clone());
        // }
        // self.coap_client = coap_client;
        Ok(())
    }

    async fn update_template_conf(
        &mut self,
        _conf: serde_json::Value,
        _23: serde_json::Value,
    ) -> HaliaResult<()> {
        todo!()
        // let old_conf: CoapConf = serde_json::from_value(old_conf)?;
        // let new_conf: CoapConf = serde_json::from_value(new_conf)?;
        // let coap_client =
        //     Arc::new(UdpCoAPClient::new_udp((new_conf.host.clone(), new_conf.port)).await?);
        // for mut source in self.sources.iter_mut() {
        //     _ = source.update_coap_client(coap_client.clone());
        // }
        // for mut sink in self.sinks.iter_mut() {
        //     _ = sink.update_coap_client(coap_client.clone());
        // }
        // self.coap_client = coap_client;
        // Ok(())
    }

    async fn stop(&mut self) {
        for mut source in self.sources.iter_mut() {
            source.stop().await;
        }
        for mut sink in self.sinks.iter_mut() {
            sink.stop().await;
        }
    }

    async fn create_customize_source(
        &mut self,
        source_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SourceConf = serde_json::from_value(conf)?;
        let source = Source::new(conf, self.coap_client.clone(), self.token_manager.clone()).await;
        self.sources.insert(source_id, source);
        Ok(())
    }

    async fn create_template_source(
        &mut self,
        source_id: String,
        conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SourceConf = serde_json::from_value(conf)?;
        let source = Source::new(conf, self.coap_client.clone(), self.token_manager.clone()).await;
        self.sources.insert(source_id, source);
        Ok(())
    }

    async fn update_customize_source(
        &mut self,
        source_id: &String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SourceConf = serde_json::from_value(conf)?;
        match self.sources.get_mut(source_id) {
            Some(mut source) => {
                source.update_conf(conf).await?;
                Ok(())
            }
            None => Err(HaliaError::NotFound(source_id.to_owned())),
        }
    }

    async fn update_template_source(
        &mut self,
        source_id: &String,
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        todo!()
        // let old_conf: SourceConf = serde_json::from_value(old_conf)?;
        // let new_conf: SourceConf = serde_json::from_value(new_conf)?;
        // match self.sources.get_mut(source_id) {
        //     Some(mut source) => {
        //         source.update_conf(old_conf, new_conf).await?;
        //         Ok(())
        //     }
        //     None => Err(HaliaError::NotFound(source_id.to_owned())),
        // }
    }

    async fn write_source_value(&mut self, _source_id: String, _req: Value) -> HaliaResult<()> {
        coap_not_support_write_source_value!()
    }

    async fn delete_source(&mut self, source_id: &String) -> HaliaResult<()> {
        match self.sources.remove(source_id) {
            Some((_, mut source)) => {
                source.stop().await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(source_id.to_owned())),
        }
    }

    async fn create_customize_sink(
        &mut self,
        sink_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf)?;
        let sink = Sink::new(self.coap_client.clone(), conf, self.token_manager.clone()).await;
        self.sinks.insert(sink_id, sink);
        Ok(())
    }

    async fn create_template_sink(
        &mut self,
        sink_id: String,
        conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf)?;
        let sink = Sink::new(self.coap_client.clone(), conf, self.token_manager.clone()).await;
        self.sinks.insert(sink_id, sink);
        Ok(())
    }

    async fn update_customize_sink(
        &mut self,
        sink_id: &String,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        // let old_conf: SinkConf = serde_json::from_value(old_conf)?;
        // let new_conf: SinkConf = serde_json::from_value(new_conf)?;
        // match self.sinks.get_mut(sink_id) {
        //     Some(mut sink) => {
        //         sink.update_conf(old_conf, new_conf).await;
        //         Ok(())
        //     }
        //     None => Err(HaliaError::NotFound(sink_id.to_owned())),
        // }
        todo!()
    }

    async fn update_template_sink(
        &mut self,
        sink_id: &String,
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        todo!()
        // let old_conf: SinkConf = serde_json::from_value(old_conf)?;
        // let new_conf: SinkConf = serde_json::from_value(new_conf)?;
        // match self.sinks.get_mut(sink_id) {
        //     Some(mut sink) => {
        //         sink.update_conf(old_conf, new_conf).await;
        //         Ok(())
        //     }
        //     None => Err(HaliaError::NotFound(sink_id.to_owned())),
        // }
    }

    async fn delete_sink(&mut self, sink_id: &String) -> HaliaResult<()> {
        match self.sinks.remove(sink_id) {
            Some((_, mut sink)) => {
                sink.stop().await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(sink_id.to_owned())),
        }
    }

    async fn get_source_rxs(
        &self,
        source_id: &String,
        cnt: usize,
    ) -> HaliaResult<Vec<mpsc::UnboundedReceiver<RuleMessageBatch>>> {
        todo!()
        // match self.sources.get(source_id) {
        //     Some(source) => Ok(source.mb_tx.subscribe()),
        //     None => Err(HaliaError::NotFound(source_id.to_owned())),
        // }
    }

    async fn get_sink_txs(
        &self,
        sink_id: &String,
        cnt: usize,
    ) -> HaliaResult<Vec<mpsc::UnboundedSender<RuleMessageBatch>>> {
        // match self.sinks.get(sink_id) {
        //     Some(sink) => Ok(sink.mb_tx.clone()),
        //     None => Err(HaliaError::NotFound(sink_id.to_owned())),
        // }
        todo!()
    }
}

pub(crate) fn transform_options(
    input_options: &Vec<(types::devices::device::coap::CoapOption, String)>,
) -> Result<Vec<(CoapOption, Vec<u8>)>> {
    let mut options = vec![];
    for (k, v) in input_options {
        let v = BASE64_STANDARD.decode(&v)?;
        match k {
            types::devices::device::coap::CoapOption::IfMatch => {
                options.push((CoapOption::IfMatch, v))
            }
            types::devices::device::coap::CoapOption::UriHost => {
                options.push((CoapOption::UriHost, v))
            }
            types::devices::device::coap::CoapOption::ETag => options.push((CoapOption::ETag, v)),
            types::devices::device::coap::CoapOption::IfNoneMatch => {
                options.push((CoapOption::IfNoneMatch, v))
            }
            types::devices::device::coap::CoapOption::Observe => {
                options.push((CoapOption::Observe, v))
            }
            types::devices::device::coap::CoapOption::UriPort => {
                options.push((CoapOption::UriPort, v))
            }
            types::devices::device::coap::CoapOption::LocationPath => {
                options.push((CoapOption::LocationPath, v))
            }
            types::devices::device::coap::CoapOption::Oscore => {
                options.push((CoapOption::Oscore, v))
            }
            types::devices::device::coap::CoapOption::UriPath => {
                options.push((CoapOption::UriPath, v))
            }
            types::devices::device::coap::CoapOption::ContentFormat => {
                options.push((CoapOption::ContentFormat, v))
            }
            types::devices::device::coap::CoapOption::MaxAge => {
                options.push((CoapOption::MaxAge, v))
            }
            types::devices::device::coap::CoapOption::UriQuery => {
                options.push((CoapOption::UriQuery, v))
            }
            types::devices::device::coap::CoapOption::Accept => {
                options.push((CoapOption::Accept, v))
            }
            types::devices::device::coap::CoapOption::LocationQuery => {
                options.push((CoapOption::LocationQuery, v))
            }
            types::devices::device::coap::CoapOption::Block2 => {
                options.push((CoapOption::Block2, v))
            }
            types::devices::device::coap::CoapOption::Block1 => {
                options.push((CoapOption::Block1, v))
            }
            types::devices::device::coap::CoapOption::ProxyUri => {
                options.push((CoapOption::ProxyUri, v))
            }
            types::devices::device::coap::CoapOption::ProxyScheme => {
                options.push((CoapOption::ProxyScheme, v))
            }
            types::devices::device::coap::CoapOption::Size1 => options.push((CoapOption::Size1, v)),
            types::devices::device::coap::CoapOption::Size2 => options.push((CoapOption::Size2, v)),
            types::devices::device::coap::CoapOption::NoResponse => {
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
