use std::str::FromStr;

use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageBatch;
use sink::Sink;
use tokio::sync::mpsc;
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

pub struct HttpClient {
    pub id: Uuid,

    on: bool,
    err: Option<String>,
    conf: CreateUpdateHttpClientReq,
    stop_signal_tx: Option<mpsc::Sender<()>>,

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
            stop_signal_tx: None,
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
            // todo 更新sink里面的信息
        }
        Ok(())
    }

    pub async fn start(&mut self) {
        match self.on {
            true => return,
            false => self.on = true,
        }
    }

    pub async fn stop(&mut self) {
        match self.on {
            true => self.on = false,
            false => return,
        }
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        todo!()
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
                Ok(restart) => {
                    todo!()
                }
                Err(e) => Err(e),
            },
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => {
                sink.delete(&self.id).await?;
                self.sinks.retain(|sink| sink.id == sink_id);
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn publish(&mut self, sink_id: &Uuid) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        // match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
        //     Some(sink) => {
        //         if sink.stop_signal_tx.is_none() {
        //             match self.conf.version {
        //                 types::apps::mqtt_client::Version::V311 => {
        //                     sink.start_v311(self.client_v311.as_ref().unwrap().clone())
        //                 }
        //                 types::apps::mqtt_client::Version::V50 => {
        //                     sink.start_v50(self.client_v50.as_ref().unwrap().clone())
        //                 }
        //             }
        //         }

        //         Ok(sink.tx.as_ref().unwrap().clone())
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    async fn unpublish(&mut self, sink_id: &Uuid) -> HaliaResult<()> {
        // match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
        //     Some(sink) => Ok(sink.unpublish().await),
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }
}
