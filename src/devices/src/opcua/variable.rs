use std::{sync::Arc, time::Duration};

use common::{error::HaliaResult, persistence};
use opcua::{
    client::Session,
    types::{DataValue, Variant},
};
use tokio::{select, sync::mpsc, time};
use types::devices::opcua::{CreateUpdateVariableReq, SearchVariablesItemResp};
use uuid::Uuid;

#[derive(Debug)]
pub struct Variable {
    pub id: Uuid,

    pub conf: CreateUpdateVariableReq,
    pub stop_signal_tx: Option<mpsc::Sender<()>>,

    on: bool,
    pub value: Option<Variant>,
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
        })
    }

    pub fn search(&self) -> SearchVariablesItemResp {
        SearchVariablesItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub async fn start(&mut self) {
        if self.on {
            return;
        } else {
            self.on = true;
        }

        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);
        // self.event_loop(self.conf, stop_signal_rx, client).await;
    }

    async fn event_loop(
        &mut self,
        interval: u64,
        mut stop_signal_rx: mpsc::Receiver<()>,
        client: Arc<Session>,
    ) {
        let handle = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return
                    }

                    _ = interval.tick() => {

                    }
                }
            }
        });
    }

    pub async fn update(&mut self, req: CreateUpdateVariableReq) -> HaliaResult<()> {
        todo!()
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
}
