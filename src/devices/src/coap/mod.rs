use anyhow::Result;
use async_trait::async_trait;
use base64::{prelude::BASE64_STANDARD, Engine as _};
use common::{
    active_sink_ref, active_source_ref, add_sink_ref, add_source_ref, check_and_set_on_false,
    check_and_set_on_true, check_delete, check_delete_sink, check_delete_source, check_stop,
    deactive_sink_ref, del_sink_ref, del_source_ref,
    error::{HaliaError, HaliaResult},
    ref_info::RefInfo,
};
use message::MessageBatch;
use protocol::coap::request::CoapOption;
use sink::Sink;
use source::Source;
use tokio::sync::{broadcast, mpsc};
use types::{
    devices::{
        coap::{CoapConf, SinkConf, SourceConf},
        CreateUpdateDeviceReq, DeviceConf, DeviceType, QueryParams, SearchDevicesItemCommon,
        SearchDevicesItemConf, SearchDevicesItemResp,
    },
    BaseConf, CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksItemResp,
    SearchSourcesOrSinksResp, Value,
};
use uuid::Uuid;

use crate::{sink_not_found_err, source_not_found_err, Device};

mod api;
mod observe;
mod sink;
mod source;

macro_rules! coap_not_support_write_source_value {
    () => {
        Err(HaliaError::Common("coap设备不支持写入源数据!".to_owned()))
    };
}

struct Coap {
    id: Uuid,
    base_conf: BaseConf,
    ext_conf: CoapConf,

    sources: Vec<Source>,
    sources_ref_infos: Vec<(Uuid, RefInfo)>,
    sinks: Vec<Sink>,
    sinks_ref_infos: Vec<(Uuid, RefInfo)>,

    on: bool,
    err: Option<String>,
}

pub async fn new(id: Uuid, device_conf: DeviceConf) -> HaliaResult<Box<dyn Device>> {
    let ext_conf: CoapConf = serde_json::from_value(device_conf.ext)?;
    Coap::validate_conf(&ext_conf)?;

    Ok(Box::new(Coap {
        id,
        base_conf: device_conf.base,
        ext_conf,
        sources: vec![],
        sources_ref_infos: vec![],
        sinks: vec![],
        sinks_ref_infos: vec![],
        on: false,
        err: None,
    }))
}

impl Coap {
    fn validate_conf(_conf: &CoapConf) -> HaliaResult<()> {
        Ok(())
    }

    fn check_on(&self) -> HaliaResult<()> {
        match self.on {
            true => Ok(()),
            false => Err(HaliaError::Stopped(format!(
                "coap设备:{}",
                self.base_conf.name
            ))),
        }
    }
}

#[async_trait]
impl Device for Coap {
    fn get_id(&self) -> &Uuid {
        &self.id
    }

    fn check_duplicate(&self, req: &CreateUpdateDeviceReq) -> HaliaResult<()> {
        if self.base_conf.name == req.conf.base.name {
            return Err(HaliaError::NameExists);
        }

        Ok(())
    }

    async fn search(&self) -> SearchDevicesItemResp {
        SearchDevicesItemResp {
            common: SearchDevicesItemCommon {
                id: self.id.clone(),
                device_type: DeviceType::Modbus,
                // rtt: self.rtt.load(Ordering::SeqCst),
                rtt: 0,
                on: self.on,
                // err: self.err.read().await.clone(),
                err: self.err.clone(),
            },
            conf: SearchDevicesItemConf {
                base: self.base_conf.clone(),
                ext: serde_json::json!(self.ext_conf),
            },
        }
    }

    async fn update(&mut self, device_conf: DeviceConf) -> HaliaResult<()> {
        let ext_conf: CoapConf = serde_json::from_value(device_conf.ext)?;
        Self::validate_conf(&ext_conf)?;

        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = device_conf.base;
        self.ext_conf = ext_conf;

        if self.on && restart {
            for source in self.sources.iter_mut() {
                _ = source.restart(&self.ext_conf);
            }
            for sink in self.sinks.iter_mut() {
                _ = sink.restart(&self.ext_conf);
            }
        }

        Ok(())
    }

    async fn delete(&mut self) -> HaliaResult<()> {
        check_delete!(self, sources_ref_infos);
        check_delete!(self, sinks_ref_infos);

        if self.on {
            self.stop().await?;
        }

        Ok(())
    }

    async fn create_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SourceConf = serde_json::from_value(req.ext)?;
        for source in self.sources.iter() {
            source.check_duplicate(&req.base, &ext_conf)?;
        }
        let mut source = Source::new(source_id, req.base, ext_conf)?;
        if self.on {
            _ = source.start(&self.ext_conf).await;
        }

        self.sources.push(source);
        self.sources_ref_infos.push((source_id, RefInfo::new()));
        Ok(())
    }

    async fn search_sources(
        &self,
        pagination: Pagination,
        query: QueryParams,
    ) -> SearchSourcesOrSinksResp {
        let mut total = 0;
        let mut data = vec![];
        for (index, source) in self.sources.iter().rev().enumerate() {
            let source = source.search();
            if let Some(name) = &query.name {
                if !source.conf.base.name.contains(name) {
                    continue;
                }
            }

            if pagination.check(total) {
                unsafe {
                    data.push(SearchSourcesOrSinksItemResp {
                        info: source,
                        rule_ref: self.sources_ref_infos.get_unchecked(index).1.get_rule_ref(),
                    });
                }
            }

            total += 1;
        }

        SearchSourcesOrSinksResp { total, data }
    }

    async fn update_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SourceConf = serde_json::from_value(req.ext)?;
        for source in self.sources.iter() {
            if source.id != source_id {
                source.check_duplicate(&req.base, &ext_conf)?;
            }
        }

        match self
            .sources
            .iter_mut()
            .find(|source| source.id == source_id)
        {
            Some(source) => source.update(req.base, ext_conf, &self.ext_conf).await,
            None => source_not_found_err!(),
        }
    }

    async fn delete_source(&mut self, source_id: Uuid) -> HaliaResult<()> {
        check_delete_source!(self, source_id);

        if self.on {
            match self
                .sources
                .iter_mut()
                .find(|source| source.id == source_id)
            {
                Some(source) => source.stop().await,
                None => unreachable!(),
            }
        }

        self.sources.retain(|source| source.id != source_id);
        self.sources_ref_infos.retain(|(id, _)| *id != source_id);
        Ok(())
    }

    async fn create_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SinkConf = serde_json::from_value(req.ext)?;

        for sink in self.sinks.iter() {
            sink.check_duplicate(&req.base, &ext_conf)?;
        }

        let mut sink = Sink::new(sink_id, req.base, ext_conf)?;
        if self.on {
            _ = sink.start(&self.ext_conf).await;
        }

        self.sinks.push(sink);
        self.sinks_ref_infos.push((sink_id, RefInfo::new()));

        Ok(())
    }

    async fn search_sinks(
        &self,
        pagination: Pagination,
        query: QueryParams,
    ) -> SearchSourcesOrSinksResp {
        let mut total = 0;
        let mut data = vec![];
        for (index, sink) in self.sinks.iter().rev().enumerate() {
            let sink = sink.search();

            if let Some(name) = &query.name {
                if !sink.conf.base.name.contains(name) {
                    continue;
                }
            }

            if pagination.check(total) {
                unsafe {
                    data.push(SearchSourcesOrSinksItemResp {
                        info: sink,
                        rule_ref: self.sinks_ref_infos.get_unchecked(index).1.get_rule_ref(),
                    })
                }
            }

            total += 1;
        }

        SearchSourcesOrSinksResp { total, data }
    }

    async fn update_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SinkConf = serde_json::from_value(req.ext)?;

        for sink in self.sinks.iter() {
            if sink.id != sink_id {
                sink.check_duplicate(&req.base, &ext_conf)?;
            }
        }

        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => sink.update(req.base, ext_conf).await,
            None => sink_not_found_err!(),
        }
    }

    async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        check_delete_sink!(self, sink_id);

        if self.on {
            match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
                Some(sink) => sink.stop().await,
                None => unreachable!(),
            }
        }

        self.sinks.retain(|sink| sink.id != sink_id);
        self.sinks_ref_infos.retain(|(id, _)| *id != sink_id);
        Ok(())
    }

    async fn add_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        add_source_ref!(self, source_id, rule_id)
    }

    async fn get_source_rx(
        &mut self,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        self.check_on()?;
        active_source_ref!(self, source_id, rule_id);
        match self
            .sources
            .iter_mut()
            .find(|source| source.id == *source_id)
        {
            Some(source) => Ok(source.mb_tx.as_ref().unwrap().subscribe()),
            None => unreachable!(),
        }
    }

    async fn del_source_rx(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        del_source_ref!(self, source_id, rule_id)
    }

    async fn del_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        del_source_ref!(self, source_id, rule_id)
    }

    async fn add_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        add_sink_ref!(self, sink_id, rule_id)
    }

    async fn get_sink_tx(
        &mut self,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        self.check_on()?;
        active_sink_ref!(self, sink_id, rule_id);
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.mb_tx.as_ref().unwrap().clone()),
            None => unreachable!(),
        }
    }

    async fn del_sink_tx(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        deactive_sink_ref!(self, sink_id, rule_id)
    }

    async fn del_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        del_sink_ref!(self, sink_id, rule_id)
    }

    async fn start(&mut self) -> HaliaResult<()> {
        check_and_set_on_true!(self);

        for source in self.sources.iter_mut() {
            _ = source.start(&self.ext_conf).await;
        }

        for sink in self.sinks.iter_mut() {
            _ = sink.start(&self.ext_conf).await;
        }

        Ok(())
    }

    async fn stop(&mut self) -> HaliaResult<()> {
        check_stop!(self, sources_ref_infos);
        check_stop!(self, sinks_ref_infos);

        check_and_set_on_false!(self);

        for source in self.sources.iter_mut() {
            _ = source.stop().await;
        }
        for sink in self.sinks.iter_mut() {
            _ = sink.stop().await;
        }

        Ok(())
    }

    async fn write_source_value(&mut self, _source_id: Uuid, _req: Value) -> HaliaResult<()> {
        coap_not_support_write_source_value!()
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
