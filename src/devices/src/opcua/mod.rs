use std::{str::FromStr, sync::Arc, time::Duration};

use common::{
    error::{HaliaError, HaliaResult},
    get_id,
    persistence::{self, Status},
};
use event::Event;
use group::Group;
use message::MessageBatch;
use opcua::{
    client::{ClientBuilder, IdentityToken, Session},
    types::{EndpointDescription, StatusCode},
};
use sink::Sink;
use subscription::Subscription;
use tokio::{
    select,
    sync::{broadcast, mpsc, RwLock},
    task::JoinHandle,
    time,
};
use tracing::debug;
use types::{
    devices::{
        opcua::{
            CreateUpdateEventReq, CreateUpdateGroupReq, CreateUpdateGroupVariableReq,
            CreateUpdateOpcuaReq, CreateUpdateSinkReq, CreateUpdateSubscriptionReq, OpcuaConf,
            SearchEventsResp, SearchGroupVariablesResp, SearchGroupsResp, SearchSinksResp,
            SearchSubscriptionsResp,
        },
        SearchDevicesItemConf, SearchDevicesItemResp,
    },
    Pagination,
};
use uuid::Uuid;

pub const TYPE: &str = "opcua";
mod event;
mod group;
mod group_variable;
pub mod manager;
mod sink;
mod subscription;

fn group_not_find_err(group_id: Uuid) -> HaliaError {
    HaliaError::NotFound("组".to_owned(), group_id)
}

struct Opcua {
    id: Uuid,
    conf: CreateUpdateOpcuaReq,

    on: bool,
    err: Option<String>,
    stop_signal_tx: Option<mpsc::Sender<()>>,
    session: Arc<RwLock<Option<Arc<Session>>>>,

    groups: Arc<RwLock<Vec<Group>>>,
    subscriptions: Vec<Subscription>,
    events: Vec<Event>,

    sinks: Vec<Sink>,
}

impl Opcua {
    pub async fn new(device_id: Option<Uuid>, req: CreateUpdateOpcuaReq) -> HaliaResult<Self> {
        let (device_id, new) = get_id(device_id);
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
            err: None,
            conf: req,
            session: Arc::new(RwLock::new(None)),
            stop_signal_tx: None,
            groups: Arc::new(RwLock::new(vec![])),
            subscriptions: vec![],
            events: vec![],
            sinks: vec![],
        })
    }

    pub fn check_duplicate_name(&self, device_id: &Option<Uuid>, name: &str) -> bool {
        if let Some(device_id) = device_id {
            if *device_id == self.id {
                return false;
            }
        }

        self.conf.base.name == name
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

    async fn recover(&mut self) -> HaliaResult<()> {
        let group_datas = persistence::devices::opcua::read_groups(&self.id).await?;
        for group_data in group_datas {
            if group_data.len() == 0 {
                continue;
            }

            let items = group_data
                .split(persistence::DELIMITER)
                .collect::<Vec<&str>>();
            assert_eq!(items.len(), 2);

            let group_id = Uuid::from_str(items[0]).unwrap();
            let req: CreateUpdateGroupReq = serde_json::from_str(items[1])?;
            self.create_group(Some(group_id), req).await?;
        }

        for group in self.groups.write().await.iter_mut() {
            group.recover(&self.id).await?;
        }

        Ok(())
    }

    fn search(&self) -> SearchDevicesItemResp {
        SearchDevicesItemResp {
            id: self.id,
            typ: TYPE,
            on: self.on,
            err: self.err.clone(),
            rtt: 9999,
            conf: SearchDevicesItemConf {
                base: self.conf.base.clone(),
                ext: serde_json::json!(self.conf.ext),
            },
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
        let opcua_conf = self.conf.ext.clone();
        let global_session = self.session.clone();
        let reconnect = self.conf.ext.reconnect;
        let groups = self.groups.clone();
        tokio::spawn(async move {
            loop {
                match Opcua::connect(&opcua_conf).await {
                    Ok((session, join_handle)) => {
                        for group in groups.write().await.iter_mut() {
                            group.start(session.clone()).await;
                        }

                        *(global_session.write().await) = Some(session);
                        match join_handle.await {
                            Ok(s) => {
                                debug!("{}", s);
                            }
                            Err(e) => debug!("{}", e),
                        }
                    }
                    Err(e) => {
                        let sleep = time::sleep(Duration::from_secs(reconnect));
                        tokio::pin!(sleep);
                        select! {
                            _ = stop_signal_rx.recv() => {
                                return
                            }

                            _ = &mut sleep => {}
                        }
                        debug!("{e}");
                    }
                }
            }
        });
    }

    async fn stop(&mut self) -> HaliaResult<()> {
        if !self.on {
            return Ok(());
        } else {
            self.on = false;
        }

        persistence::devices::update_device_status(&self.id, Status::Stopped).await?;

        match self
            .session
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

    async fn update(&mut self, req: CreateUpdateOpcuaReq) -> HaliaResult<()> {
        persistence::devices::update_device_conf(&self.id, serde_json::to_string(&req).unwrap())
            .await?;

        let mut restart = false;
        if self.conf.ext != req.ext {
            restart = true;
        }
        self.conf = req;

        if restart && self.on {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();
        }

        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        Ok(())
    }

    async fn create_group(
        &mut self,
        group_id: Option<Uuid>,
        req: CreateUpdateGroupReq,
    ) -> HaliaResult<()> {
        match Group::new(&self.id, group_id, req).await {
            Ok(mut group) => {
                if self.on && self.session.read().await.is_some() {
                    group
                        .start(self.session.read().await.as_ref().unwrap().clone())
                        .await;
                }
                self.groups.write().await.push(group);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn search_groups(&self, pagination: Pagination) -> HaliaResult<SearchGroupsResp> {
        let mut data = vec![];
        for group in self
            .groups
            .read()
            .await
            .iter()
            .rev()
            .skip((pagination.page - 1) * pagination.size)
        {
            data.push(group.search().await);
            if data.len() == pagination.size {
                break;
            }
        }

        Ok(SearchGroupsResp {
            total: self.groups.read().await.len(),
            data,
        })
    }

    async fn update_group(&self, group_id: Uuid, req: CreateUpdateGroupReq) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.update(&self.id, req).await,
            None => Err(group_not_find_err(group_id)),
        }
    }

    async fn delete_group(&self, group_id: Uuid) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => {
                group.delete().await?;
            }
            None => return Err(group_not_find_err(group_id)),
        }

        self.groups
            .write()
            .await
            .retain(|group| group.id != group_id);
        Ok(())
    }

    async fn create_group_variable(
        &mut self,
        group_id: Uuid,
        variable_id: Option<Uuid>,
        req: CreateUpdateGroupVariableReq,
    ) -> HaliaResult<()> {
        match self
            .groups
            .read()
            .await
            .iter()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.create_variable(&self.id, variable_id, req).await,
            None => Err(group_not_find_err(group_id)),
        }
    }

    pub async fn read_group_variables(
        &self,
        group_id: Uuid,
        pagination: Pagination,
    ) -> HaliaResult<SearchGroupVariablesResp> {
        match self
            .groups
            .read()
            .await
            .iter()
            .find(|group| group.id == group_id)
        {
            Some(group) => Ok(group.read_variables(pagination).await),
            None => Err(group_not_find_err(group_id)),
        }
    }

    pub async fn update_group_variable(
        &self,
        group_id: Uuid,
        variable_id: Uuid,
        req: CreateUpdateGroupVariableReq,
    ) -> HaliaResult<()> {
        match self
            .groups
            .read()
            .await
            .iter()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.update_variable(&self.id, variable_id, req).await,
            None => Err(group_not_find_err(group_id)),
        }
    }

    pub async fn delete_group_variable(
        &self,
        group_id: Uuid,
        variable_id: Uuid,
    ) -> HaliaResult<()> {
        match self
            .groups
            .read()
            .await
            .iter()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.delete_variable(&self.id, variable_id).await,
            None => Err(group_not_find_err(group_id)),
        }
    }

    pub async fn add_group_ref(&self, group_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == *group_id)
        {
            Some(group) => Ok(group.add_ref(rule_id)),
            None => Err(group_not_find_err(group_id.clone())),
        }
    }

    async fn get_group_mb_rx(
        &self,
        group_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == *group_id)
        {
            Some(group) => Ok(group.get_mb_rx(rule_id)),
            None => Err(group_not_find_err(group_id.clone())),
        }
    }

    async fn del_group_mb_rx(&self, group_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == *group_id)
        {
            Some(group) => Ok(group.del_mb_rx(rule_id)),
            None => Err(group_not_find_err(group_id.clone())),
        }
    }

    pub async fn del_group_ref(&self, group_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == *group_id)
        {
            Some(group) => Ok(group.del_ref(rule_id)),
            None => Err(group_not_find_err(group_id.clone())),
        }
    }

    async fn create_subscription(
        &mut self,
        subscription_id: Option<Uuid>,
        req: CreateUpdateSubscriptionReq,
    ) -> HaliaResult<()> {
        match Subscription::new(&self.id, subscription_id, req).await {
            Ok(mut subscription) => {
                if self.on && self.session.read().await.is_some() {
                    subscription
                        .start(self.session.read().await.as_ref().unwrap().clone())
                        .await;
                }
                self.subscriptions.push(subscription);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn search_subscriptions(
        &self,
        pagination: Pagination,
    ) -> HaliaResult<SearchSubscriptionsResp> {
        let mut data = vec![];
        for subscription in self
            .subscriptions
            .iter()
            .rev()
            .skip((pagination.page - 1) * pagination.size)
        {
            data.push(subscription.search());
            if data.len() == pagination.size {
                break;
            }
        }

        Ok(SearchSubscriptionsResp {
            total: self.subscriptions.len(),
            data,
        })
    }

    async fn update_subscription(
        &self,
        subscription_id: Uuid,
        req: CreateUpdateSubscriptionReq,
    ) -> HaliaResult<()> {
        // match self
        //     .groups
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|group| group.id == group_id)
        // {
        //     Some(group) => group.update(&self.id, req).await,
        //     None => Err(group_not_find_err(group_id)),
        // }
        todo!()
    }

    async fn delete_subscription(&self, subscription_id: Uuid) -> HaliaResult<()> {
        // match self
        //     .groups
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|group| group.id == group_id)
        // {
        //     Some(group) => {
        //         group.delete().await?;
        //     }
        //     None => return Err(group_not_find_err(group_id)),
        // }

        // self.groups
        //     .write()
        //     .await
        //     .retain(|group| group.id != group_id);
        // Ok(())
        todo!()
    }

    async fn create_event(
        &mut self,
        event_id: Option<Uuid>,
        req: CreateUpdateEventReq,
    ) -> HaliaResult<()> {
        match Event::new(&self.id, event_id, req).await {
            Ok(mut event) => {
                if self.on && self.session.read().await.is_some() {
                    // event
                    //     .start(self.session.read().await.as_ref().unwrap().clone())
                    //     .await;
                }
                self.events.push(event);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn search_events(&self, pagination: Pagination) -> HaliaResult<SearchEventsResp> {
        let mut data = vec![];
        for event in self
            .events
            .iter()
            .rev()
            .skip((pagination.page - 1) * pagination.size)
        {
            data.push(event.search());
            if data.len() == pagination.size {
                break;
            }
        }

        Ok(SearchEventsResp {
            total: self.events.len(),
            data,
        })
    }

    async fn update_event(&mut self, event_id: Uuid, req: CreateUpdateEventReq) -> HaliaResult<()> {
        match self.events.iter_mut().find(|event| event.id == event_id) {
            Some(event) => event.update(&self.id, req).await,
            None => Err(group_not_find_err(event_id)),
        }
    }

    async fn delete_event(&self, event_id: Uuid) -> HaliaResult<()> {
        // match self
        //     .groups
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|group| group.id == group_id)
        // {
        //     Some(group) => {
        //         group.delete().await?;
        //     }
        //     None => return Err(group_not_find_err(group_id)),
        // }

        // self.groups
        //     .write()
        //     .await
        //     .retain(|group| group.id != group_id);
        // Ok(())
        todo!()
    }

    async fn create_sink(
        &mut self,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match Sink::new(&self.id, sink_id, req).await {
            Ok(sink) => {
                if self.on {
                    //
                }
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

    async fn update_sink(&mut self, sink_id: Uuid, req: CreateUpdateSinkReq) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => sink.update(&self.id, req).await,
            None => todo!(),
        }
    }

    async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => {
                sink.delete(&self.id).await?;
                self.sinks.retain(|sink| sink.id != sink_id);
                Ok(())
            }
            None => todo!(),
        }
    }
}
