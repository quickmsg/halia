use bytes::Bytes;
use common::{
    error::{HaliaError, HaliaResult},
    persistence::{self, Status},
};
use message::MessageBatch;
use opcua::{
    client::{ClientBuilder, IdentityToken, Session},
    types::{EndpointDescription, StatusCode},
};
use serde_json::json;
use std::{
    env::var,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tracing::debug;
use types::devices::{
    opcua::{CreateUpdateOpcuaReq, CreateUpdateVariableReq, OpcuaConf, SearchVariablesResp},
    SearchDevicesItemResp,
};
use variable::Variable;

use uuid::Uuid;

pub const TYPE: &str = "opcua";
pub mod manager;
mod variable;

struct Opcua {
    id: Uuid,
    err: Arc<AtomicBool>,
    conf: CreateUpdateOpcuaReq,

    variables: Vec<Variable>,

    on: bool,
    stop_signal_tx: Option<mpsc::Sender<()>>,
    session: Option<Arc<Session>>,
}

impl Opcua {
    pub async fn new(device_id: Option<Uuid>, req: CreateUpdateOpcuaReq) -> HaliaResult<Self> {
        let (device_id, new) = match device_id {
            Some(device_id) => (device_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::devices::opcua::create(
                &device_id,
                TYPE,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Opcua {
            id: device_id,
            on: false,
            err: Arc::new(AtomicBool::new(false)),
            conf: req,
            session: None,
            stop_signal_tx: None,
            variables: vec![],
        })
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
                return Err(HaliaError::IoErr);
            }
        };

        let handle = event_loop.spawn();
        session.wait_for_connection().await;
        Ok((session, handle))
    }

    async fn recover(&mut self) -> HaliaResult<()> {
        todo!()
    }

    fn search(&self) -> SearchDevicesItemResp {
        SearchDevicesItemResp {
            id: self.id,
            r#type: TYPE,
            on: self.on,
            err: self.err.load(Ordering::SeqCst),
            rtt: 9999,
            conf: json!(&self.conf),
        }
    }

    async fn start(&mut self) -> HaliaResult<()> {
        if self.on {
            return Ok(());
        } else {
            self.on = true;
        }

        persistence::devices::update_device_status(&self.id, Status::Runing).await?;

        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        self.event_loop(stop_signal_rx).await;
        Ok(())
    }

    async fn event_loop(&mut self, mut stop_signal_rx: mpsc::Receiver<()>) {
        let opcua_conf = self.conf.opcua_conf.clone();
        loop {
            match Opcua::connect(&opcua_conf).await {
                Ok((session, join_handle)) => {}
                Err(_) => todo!(),
            }
        }
    }

    async fn stop(&mut self) -> HaliaResult<()> {
        if !self.on {
            return Ok(());
        } else {
            self.on = false;
        }

        persistence::devices::update_device_status(&self.id, Status::Stopped).await?;

        match self.session.as_ref().unwrap().disconnect().await {
            Ok(_) => {
                debug!("session disconnect success");
            }
            Err(e) => {
                debug!("err code is :{}", e);
            }
        }

        Ok(())
    }

    async fn update(&mut self, req: CreateUpdateOpcuaReq) -> HaliaResult<()> {
        // if self.name != req.name {
        //     self.name = req.name.clone();
        // }

        // let mut conf = self.conf.write().await;
        // if conf.url != new_conf.url {
        //     conf.url = new_conf.url;
        //     let _ = self
        //         .session
        //         .write()
        //         .await
        //         .as_ref()
        //         .unwrap()
        //         .disconnect()
        //         .await;
        // }
        // restart

        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        Ok(())
    }

    async fn create_variable(
        &mut self,
        variable_id: Option<Uuid>,
        req: CreateUpdateVariableReq,
    ) -> HaliaResult<()> {
        match Variable::new(&self.id, variable_id, req).await {
            Ok(mut variable) => {
                if self.on {
                    variable.start(self.session.as_ref().unwrap().clone()).await;
                }
                self.variables.push(variable);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn search_variables(&self, page: usize, size: usize) -> HaliaResult<SearchVariablesResp> {
        let mut data = vec![];
        for varibale in self.variables.iter().rev().skip((page - 1) * size) {
            data.push(varibale.search());
            if data.len() == size {
                break;
            }
        }

        Ok(SearchVariablesResp {
            total: self.variables.len(),
            data,
        })
    }

    async fn update_variable(
        &mut self,
        variable_id: Uuid,
        req: CreateUpdateVariableReq,
    ) -> HaliaResult<()> {
        match self
            .variables
            .iter_mut()
            .find(|variable| variable.id == variable_id)
        {
            Some(variable) => variable.update(&self.id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    async fn delete_variable(&mut self, variable_id: Uuid) -> HaliaResult<()> {
        match self
            .variables
            .iter_mut()
            .find(|variable| variable.id == variable_id)
        {
            Some(variable) => {
                variable.delete().await?;
            }
            None => return Err(HaliaError::NotFound),
        }

        self.variables.retain(|variable| variable.id != variable_id);
        Ok(())
    }

    async fn subscribe(
        &mut self,
        variable_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        todo!()
    }

    async fn unsubscribe(&mut self, group_id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    async fn create_sink(&mut self, sink_id: Uuid, req: &String) -> HaliaResult<()> {
        todo!()
    }

    async fn update_sink(&mut self, sink_id: Uuid, req: &String) -> HaliaResult<()> {
        todo!()
    }

    async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    async fn publish(&mut self, sink_id: &Uuid) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        todo!()
    }

    async fn add_subscription(&self, req: Bytes) -> HaliaResult<()> {
        // TODO
        Err(HaliaError::ParseErr)
    }
}
