use std::str::FromStr;

use common::{
    check_and_set_on_true,
    error::{HaliaError, HaliaResult},
    persistence,
};
use sink::Sink;
use types::{
    apps::{
        http_client::{CreateUpdateHttpClientReq, CreateUpdateSinkReq, SearchSinksResp},
        SearchAppsItemResp,
    },
    Pagination,
};
use uuid::Uuid;

pub const TYPE: &str = "http_client";

pub mod manager;
mod sink;

fn sink_not_find_err(sink_id: Uuid) -> HaliaError {
    HaliaError::NotFound("http客户端动作".to_owned(), sink_id)
}

pub struct HttpClient {
    pub id: Uuid,

    on: bool,
    err: Option<String>,
    conf: CreateUpdateHttpClientReq,

    sinks: Vec<Sink>,
}

impl HttpClient {
    pub async fn new(app_id: Option<Uuid>, req: CreateUpdateHttpClientReq) -> HaliaResult<Self> {
        let (app_id, new) = match app_id {
            Some(app_id) => (app_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::apps::mqtt_client::create(
                &app_id,
                TYPE,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Self {
            id: app_id,
            conf: req,
            on: false,
            err: None,
            sinks: vec![],
        })
    }

    pub async fn recover(&mut self) -> HaliaResult<()> {
        match persistence::apps::http_client::read_sinks(&self.id).await {
            Ok(datas) => {
                for data in datas {
                    if data.len() == 0 {
                        continue;
                    }
                    let items = data.split(persistence::DELIMITER).collect::<Vec<&str>>();
                    assert_eq!(items.len(), 2);
                    let sink_id = Uuid::from_str(items[0]).unwrap();
                    self.create_sink(Some(sink_id), serde_json::from_str(items[1]).unwrap())
                        .await?;
                }
            }
            Err(e) => return Err(e.into()),
        }

        Ok(())
    }

    pub async fn update(&mut self, req: CreateUpdateHttpClientReq) -> HaliaResult<()> {
        persistence::apps::update_app_conf(&self.id, serde_json::to_string(&req).unwrap()).await?;

        let mut restart = false;
        if self.conf.ext != req.ext {
            restart = true;
        }
        self.conf = req;

        if restart {
            for sink in self.sinks.iter_mut() {
                sink.restart().await;
            }
        }

        Ok(())
    }

    pub async fn start(&mut self) -> HaliaResult<()> {
        check_and_set_on_true!(self);
        for sink in self.sinks.iter_mut() {
            sink.start(self.conf.ext.host.clone()).await;
        }

        Ok(())
    }

    pub async fn stop(&mut self) -> HaliaResult<()> {
        for sink in self.sinks.iter() {
            if !sink.can_stop() {
                return Err(HaliaError::Common("动作被引用中".to_owned()));
            }
        }

        match self.on {
            true => self.on = false,
            false => return Ok(()),
        }

        for sink in self.sinks.iter_mut() {
            sink.stop().await;
        }

        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        for sink in self.sinks.iter() {
            if !sink.can_delete() {
                return Err(HaliaError::Common("动作被引用中".to_owned()));
            }
        }

        Ok(())
    }

    fn search(&self) -> SearchAppsItemResp {
        SearchAppsItemResp {
            id: self.id,
            on: self.on,
            typ: TYPE,
            conf: serde_json::to_value(&self.conf).unwrap(),
            err: self.err.clone(),
        }
    }

    async fn create_sink(
        &mut self,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match sink::new(&self.id, sink_id, req).await {
            Ok(sink) => {
                self.sinks.push(sink);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn search_sinks(&self, pagination: Pagination) -> SearchSinksResp {
        let mut data = vec![];
        for sink in self
            .sinks
            .iter()
            .rev()
            .skip((pagination.page - 1) * pagination.size)
        {
            data.push(sink.search());
            if data.len() == pagination.size {
                break;
            }
        }
        SearchSinksResp {
            total: self.sinks.len(),
            data,
        }
    }

    pub async fn update_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => match sink.update(&self.id, req).await {
                Ok(()) => Ok(()),
                Err(e) => Err(e),
            },
            None => Err(sink_not_find_err(sink_id)),
        }
    }

    pub async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => {
                sink.delete(&self.id).await?;
                self.sinks.retain(|sink| sink.id == sink_id);
                Ok(())
            }
            None => Err(sink_not_find_err(sink_id)),
        }
    }
}
