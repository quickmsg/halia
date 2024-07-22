use std::{sync::Arc, time::Duration};

use common::{error::HaliaResult, persistence};
use opcua::{
    client::Session,
    types::{DataValue, ReadValueId, TimestampsToReturn, Variant},
};
use tokio::{select, sync::mpsc, task::JoinHandle, time};
use tracing::debug;
use types::devices::opcua::{CreateUpdateVariableReq, SearchVariablesItemResp};
use uuid::Uuid;

pub struct Variable {
    pub id: Uuid,

    pub conf: CreateUpdateVariableReq,
    pub stop_signal_tx: Option<mpsc::Sender<()>>,

    on: bool,
    pub value: Option<Variant>,

    handle: Option<JoinHandle<(mpsc::Receiver<()>, Arc<Session>)>>,
}

impl Variable {
    pub async fn new(
        device_id: &Uuid,
        variable_id: Option<Uuid>,
        req: CreateUpdateVariableReq,
    ) -> HaliaResult<Self> {
        let (variable_id, new) = match variable_id {
            Some(variable_id) => (variable_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::devices::opcua::create_variable(
                device_id,
                &variable_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Self {
            id: variable_id,
            conf: req,
            value: None,
            on: false,
            stop_signal_tx: None,
            handle: None,
        })
    }

    pub fn search(&self) -> SearchVariablesItemResp {
        SearchVariablesItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub async fn update(
        &mut self,
        device_id: &Uuid,
        req: CreateUpdateVariableReq,
    ) -> HaliaResult<()> {
        persistence::devices::opcua::update_variable(
            device_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        let mut restart = false;
        if self.conf.variable_conf != req.variable_conf {
            restart = true;
        }
        self.conf = req;

        if self.on && restart {
            self.stop_signal_tx.as_ref().unwrap().send(()).await;
            let (stop_signal_rx, client) = self.handle.take().unwrap().await.unwrap();
            self.event_loop(stop_signal_rx, client);
        }

        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        todo!()
    }

    pub fn write(&mut self, data_value: Option<DataValue>) {
        match data_value {
            Some(data_value) => self.value = data_value.value,
            None => self.value = None,
        }
    }

    pub async fn start(&mut self, client: Arc<Session>) {
        if self.on {
            return;
        } else {
            self.on = true;
        }

        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);
        self.event_loop(stop_signal_rx, client).await;
    }

    async fn event_loop(&mut self, mut stop_signal_rx: mpsc::Receiver<()>, client: Arc<Session>) {
        let interval = self.conf.variable_conf.interval;
        let handle = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));
            let read_value_id = ReadValueId {
                node_id: todo!(),
                attribute_id: todo!(),
                index_range: todo!(),
                data_encoding: todo!(),
            };
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, client)
                    }

                    _ = interval.tick() => {
                        Variable::read_variable(&client, &read_value_id).await;
                    }
                }
            }
        });
        self.handle = Some(handle);
    }

    async fn read_variable(client: &Arc<Session>, read_value_id: &ReadValueId) {
        match client
            .read(&[read_value_id.clone()], TimestampsToReturn::Both, 2000.0)
            .await
        {
            Ok(resp) => {
                debug!("{:?}", resp);
            }
            Err(e) => {
                debug!("err code :{:?}", e);
            }
        }
    }
}
