use std::{sync::Arc, time::Duration};

use common::error::HaliaResult;
use futures::lock::BiLock;
use futures_util::StreamExt;
use message::RuleMessageBatch;
use reqwest::{header::HeaderName, Client, Request};
use tokio::{
    select,
    sync::{
        mpsc::{self, unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch,
    },
    task::JoinHandle,
    time,
};
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest as _};
use tracing::{debug, warn};
use types::apps::http_client::{HttpClientConf, SourceConf};
use utils::ErrorManager;

use super::{insert_basic_auth, insert_headers, insert_query};

pub struct Source {
    stop_signal_tx: watch::Sender<()>,
    err: BiLock<Option<Arc<String>>>,
    join_handle: Option<JoinHandle<TaskLoop>>,
    pub mb_txs: BiLock<Vec<UnboundedSender<RuleMessageBatch>>>,
}

pub struct TaskLoop {
    pub id: String,
    pub stop_signal_rx: watch::Receiver<()>,
    pub http_client_conf: Arc<HttpClientConf>,
    pub source_conf: SourceConf,
    pub http_client: Client,
    pub mb_txs: BiLock<Vec<UnboundedSender<RuleMessageBatch>>>,
    device_err_tx: mpsc::UnboundedSender<Option<Arc<String>>>,
    decoder: Box<dyn schema::Decoder>,
    error_manager: ErrorManager,
}

impl TaskLoop {
    async fn new(
        id: String,
        err: BiLock<Option<Arc<String>>>,
        stop_signal_rx: watch::Receiver<()>,
        http_client_conf: Arc<HttpClientConf>,
        source_conf: SourceConf,
        mb_txs: BiLock<Vec<UnboundedSender<RuleMessageBatch>>>,
        device_err_tx: mpsc::UnboundedSender<Option<Arc<String>>>,
    ) -> Self {
        let decoder = schema::new_decoder(&source_conf.decode_type, &source_conf.schema_id)
            .await
            .unwrap();
        let http_client = Client::new();
        let error_manager = ErrorManager::new(
            utils::error_manager::ResourceType::AppSource,
            id.clone(),
            err,
        );

        Self {
            id,
            stop_signal_rx,
            http_client_conf,
            source_conf,
            http_client,
            mb_txs,
            device_err_tx,
            decoder,
            error_manager,
        }
    }

    fn start(mut self) -> JoinHandle<Self> {
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(
                self.source_conf.http.as_ref().unwrap().interval,
            ));

            let mut builder = self.http_client.get(format!(
                "http://{}:{}{}",
                &self.http_client_conf.host, &self.http_client_conf.port, &self.source_conf.path
            ));

            builder = insert_headers(
                builder,
                &self.source_conf.headers,
                &self.http_client_conf.headers,
            );
            builder = insert_query(
                builder,
                &self.http_client_conf.query_params,
                &self.source_conf.query_params,
            );
            builder = insert_basic_auth(builder, &self.http_client_conf.basic_auth);

            let request = match builder.build() {
                Ok(request) => request,
                Err(e) => {
                    warn!("{:?}", e);
                    return self;
                }
            };

            loop {
                select! {
                    _ = self.stop_signal_rx.changed() => {
                        return self;
                    }

                    _ = interval.tick() => {
                        self.do_request(request.try_clone().unwrap()).await;
                    }
                }
            }
        })
    }

    async fn do_request(&mut self, request: Request) {
        if self.mb_txs.lock().await.len() == 0 {
            return;
        }

        match self.http_client.execute(request).await {
            Ok(resp) => {
                if resp.status().is_success() {
                    let status_changed = self.error_manager.set_ok().await;
                    if status_changed {
                        self.device_err_tx.send(None).unwrap();
                    }

                    match resp.bytes().await {
                        Ok(body) => match self.decoder.decode(body) {
                            Ok(mb) => {
                                let mut mb_txs = self.mb_txs.lock().await;
                                if mb_txs.len() == 1 {
                                    let rmb = RuleMessageBatch::Owned(mb);
                                    if let Err(_) = mb_txs[0].send(rmb) {
                                        mb_txs.remove(0);
                                    }
                                } else {
                                    let rmb = RuleMessageBatch::Arc(Arc::new(mb));
                                    mb_txs.retain(|tx| tx.send(rmb.clone()).is_ok());
                                }
                            }
                            Err(e) => warn!("{}", e),
                        },
                        Err(_) => todo!(),
                    }
                } else {
                    let status_code = resp.status();
                    // TODO 是否会触发unwrap
                    let body = resp.text().await.unwrap();
                    let err = Arc::new(format!("状态码：{}, 错误：{}。", status_code, body));
                    self.error_manager.put_err(err.clone());
                }
            }
            Err(e) => {
                let err = Arc::new(e.to_string());
                let status_changed = self.error_manager.put_err(err.clone()).await;
                if status_changed {
                    self.device_err_tx.send(Some(err.clone())).unwrap();
                }
            }
        }
    }
}

impl Source {
    pub async fn new(
        id: String,
        http_client_conf: Arc<HttpClientConf>,
        source_conf: SourceConf,
        device_err_tx: mpsc::UnboundedSender<Option<Arc<String>>>,
    ) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (err1, err2) = BiLock::new(None);
        let (mb_txs1, mb_txs2) = BiLock::new(vec![]);

        let task_loop = TaskLoop::new(
            id,
            err1,
            stop_signal_rx,
            http_client_conf,
            source_conf,
            mb_txs1,
            device_err_tx,
        )
        .await;

        let join_handle = task_loop.start();

        Self {
            err: err2,
            stop_signal_tx,
            join_handle: Some(join_handle),
            mb_txs: mb_txs2,
        }
    }

    pub fn validate_conf(_conf: &SourceConf) -> HaliaResult<()> {
        Ok(())
    }

    pub async fn read_err(&self) -> Option<String> {
        match &(*self.err.lock().await) {
            Some(err) => Some((**err).clone()),
            None => None,
        }
    }

    pub async fn update_conf(&mut self, _old_conf: SourceConf, new_conf: SourceConf) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.source_conf = new_conf;
        let join_handle = Self::event_loop(join_handle_data).await;
        self.join_handle = Some(join_handle);
    }

    pub async fn update_http_client(&mut self, http_client_conf: Arc<HttpClientConf>) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.http_client_conf = http_client_conf;
        let join_hdnale = Self::event_loop(join_handle_data).await;
        self.join_handle = Some(join_hdnale);
    }

    async fn event_loop(join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        match join_handle_data.source_conf.typ {
            types::apps::http_client::SourceType::Http => Self::http_event_loop(join_handle_data),
            types::apps::http_client::SourceType::Websocket => {
                Self::ws_event_loop(join_handle_data).await
            }
        }
    }

    fn http_event_loop(mut join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(
                join_handle_data.source_conf.http.as_ref().unwrap().interval,
            ));

            let mut builder = join_handle_data.client.get(format!(
                "http://{}:{}{}",
                &join_handle_data.http_client_conf.host,
                &join_handle_data.http_client_conf.port,
                &join_handle_data.source_conf.path
            ));

            builder = insert_headers(
                builder,
                &join_handle_data.source_conf.headers,
                &join_handle_data.http_client_conf.headers,
            );
            builder = insert_query(
                builder,
                &join_handle_data.http_client_conf.query_params,
                &join_handle_data.source_conf.query_params,
            );
            builder = insert_basic_auth(builder, &join_handle_data.http_client_conf.basic_auth);

            let request = match builder.build() {
                Ok(request) => request,
                Err(e) => {
                    warn!("{:?}", e);
                    return join_handle_data;
                }
            };

            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        return join_handle_data;
                    }

                    _ = interval.tick() => {
                        Self::do_request(&mut join_handle_data, request.try_clone().unwrap()).await;
                    }
                }
            }
        })
    }

    async fn do_request(join_handle_data: &mut JoinHandleData, request: Request) {
        if join_handle_data.mb_txs.lock().await.len() == 0 {
            return;
        }

        match join_handle_data.client.execute(request).await {
            Ok(resp) => {
                if resp.status().is_success() {
                    let (need_update_err, need_update_status) =
                        join_handle_data.error_manager.put_ok();
                    if need_update_err {
                        *join_handle_data.err.lock().await = None;
                    }
                    if need_update_status {
                        let _ = storage::app::source_sink::update_status(
                            &join_handle_data.id,
                            types::Status::Running,
                        )
                        .await;
                        join_handle_data.device_err_tx.send(None).unwrap();
                    }

                    match resp.bytes().await {
                        Ok(body) => match join_handle_data.decoder.decode(body) {
                            Ok(mb) => {
                                let mut mb_txs = join_handle_data.mb_txs.lock().await;
                                if mb_txs.len() == 1 {
                                    let rmb = RuleMessageBatch::Owned(mb);
                                    if let Err(_) = mb_txs[0].send(rmb) {
                                        mb_txs.remove(0);
                                    }
                                } else {
                                    let rmb = RuleMessageBatch::Arc(Arc::new(mb));
                                    mb_txs.retain(|tx| tx.send(rmb.clone()).is_ok());
                                }
                            }
                            Err(e) => warn!("{}", e),
                        },
                        Err(_) => todo!(),
                    }
                } else {
                    let status_code = resp.status();
                    // TODO 是否会触发unwrap
                    let body = resp.text().await.unwrap();
                    let err = Arc::new(format!("状态码：{}, 错误：{}。", status_code, body));
                    let (need_update_err, need_update_err_status) =
                        join_handle_data.error_manager.put_err(err.clone());
                    if need_update_err {
                        *join_handle_data.err.lock().await = Some(err);
                    }
                    if need_update_err_status {
                        let _ = storage::app::source_sink::update_status(
                            &join_handle_data.id,
                            types::Status::Error,
                        )
                        .await;
                    }
                }
            }
            Err(e) => {
                let err = Arc::new(e.to_string());
                let (need_update_err, need_update_err_status) =
                    join_handle_data.error_manager.put_err(err.clone());

                if need_update_err {
                    *join_handle_data.err.lock().await = Some(err.clone());
                    join_handle_data
                        .device_err_tx
                        .send(Some(err.clone()))
                        .unwrap();
                }
                if need_update_err_status {
                    let _ = storage::app::source_sink::update_status(
                        &join_handle_data.id,
                        types::Status::Error,
                    )
                    .await;
                }
            }
        }
    }

    async fn ws_event_loop(mut join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        tokio::spawn(async move {
            let mut task_err: Option<String> = Some("not connectd.".to_owned());
            loop {
                let request = connect_websocket(
                    &join_handle_data.http_client_conf,
                    &join_handle_data.source_conf.path,
                    &join_handle_data.source_conf.headers,
                );
                match connect_async(request).await {
                    Ok((ws_stream, response)) => {
                        if !response.status().is_success() {
                            task_err =
                                Some(format!("websocket 连接失败，状态码：{}", response.status()));
                        }
                        let (_, mut read) = ws_stream.split();
                        loop {
                            select! {
                                msg = read.next() => {
                                    debug!("msg: {:?}", msg);
                                }

                                _ = join_handle_data.stop_signal_rx.changed() => {
                                    return join_handle_data;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        task_err = Some(e.to_string());
                        let sleep = time::sleep(Duration::from_secs(
                            join_handle_data
                                .source_conf
                                .websocket
                                .as_ref()
                                .unwrap()
                                .reconnect,
                        ));
                        tokio::pin!(sleep);
                        select! {
                            _ = join_handle_data.stop_signal_rx.changed() => {
                                return join_handle_data;
                            }

                            _ = &mut sleep => {}
                        }
                    }
                }
            }
        })
    }

    pub async fn stop(&mut self) -> JoinHandleData {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }

    pub async fn get_rxs(&mut self, cnt: usize) -> Vec<UnboundedReceiver<RuleMessageBatch>> {
        let mut rxs = Vec::with_capacity(cnt);
        let mut txs = Vec::with_capacity(cnt);
        for _ in 0..cnt {
            let (tx, rx) = unbounded_channel();
            txs.push(tx);
            rxs.push(rx);
        }
        self.mb_txs.lock().await.append(&mut txs);
        rxs
    }
}

fn connect_websocket(
    conf: &Arc<HttpClientConf>,
    path: &String,
    headers: &Vec<(String, String)>,
) -> tokio_tungstenite::tungstenite::handshake::client::Request {
    match conf.ssl_enable {
        true => todo!(),
        false => {
            let mut request = format!("ws://{}:{}/{}", conf.host, conf.port, path)
                .into_client_request()
                .unwrap();

            for (key, value) in headers {
                request.headers_mut().insert(
                    // TODO 提前检查，确认不会出发panic
                    HeaderName::from_bytes(key.as_bytes()).unwrap(),
                    value.parse().unwrap(),
                );
            }

            request
        }
    }
}
