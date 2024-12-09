use std::sync::Arc;

use common::error::HaliaResult;
use futures::lock::BiLock;
use halia_derive::{ResourceErr, ResourceStop, SinkTxs};
use message::RuleMessageBatch;
use reqwest::Client;
use tokio::{
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch,
    },
    task::JoinHandle,
};
use tracing::warn;
use types::apps::http_client::{HttpClientConf, SinkConf};
use utils::ErrorManager;

use super::{build_http_client, insert_basic_auth, insert_headers, insert_query};

#[derive(ResourceErr, ResourceStop, SinkTxs)]
pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<TaskLoop>>,
    err: BiLock<Option<Arc<String>>>,
    mb_tx: UnboundedSender<RuleMessageBatch>,
}

pub struct TaskLoop {
    sink_conf: SinkConf,
    stop_signal_rx: watch::Receiver<()>,
    app_conf: Arc<HttpClientConf>,
    app_err_tx: UnboundedSender<Option<Arc<String>>>,
    http_client: Client,
    mb_rx: UnboundedReceiver<RuleMessageBatch>,
    error_manager: ErrorManager,
}

impl TaskLoop {
    pub fn new(
        sink_id: String,
        sink_conf: SinkConf,
        sink_err: BiLock<Option<Arc<String>>>,
        stop_signal_rx: watch::Receiver<()>,
        app_conf: Arc<HttpClientConf>,
        app_err_tx: UnboundedSender<Option<Arc<String>>>,
        mb_rx: UnboundedReceiver<RuleMessageBatch>,
    ) -> Self {
        let error_manager = ErrorManager::new(
            utils::error_manager::ResourceType::AppSink,
            sink_id,
            sink_err,
        );

        let http_client = build_http_client(&app_conf);
        Self {
            sink_conf,
            stop_signal_rx,
            app_conf,
            http_client,
            mb_rx,
            error_manager,
            app_err_tx,
        }
    }

    fn start(mut self) -> JoinHandle<Self> {
        let schema = match self.app_conf.ssl_enable {
            true => "https",
            false => "http",
        };
        let url = format!(
            "{}://{}:{}{}",
            schema, &self.app_conf.host, self.app_conf.port, &self.sink_conf.path
        );
        tokio::spawn(async move {
            loop {
                select! {
                    _ = self.stop_signal_rx.changed() => {
                        return self;
                    }

                    mb = self.mb_rx.recv() => {
                        match mb {
                            Some(mb) => self.send_request(&url, mb).await,
                            None => warn!("http客户端收到空消息"),
                        }
                    }
                }
            }
        })
    }

    async fn send_request(&mut self, url: &String, _mb: RuleMessageBatch) {
        // todo 重复使用

        let mut builder = match self.sink_conf.method {
            types::apps::http_client::SinkMethod::Get => self.http_client.get(url),
            types::apps::http_client::SinkMethod::Post => self.http_client.post(url),
            types::apps::http_client::SinkMethod::Delete => self.http_client.delete(url),
            types::apps::http_client::SinkMethod::Patch => self.http_client.patch(url),
            types::apps::http_client::SinkMethod::Put => self.http_client.put(url),
            types::apps::http_client::SinkMethod::Head => self.http_client.head(url),
        };

        builder = builder.query(&self.sink_conf.query_params);

        builder = insert_headers(builder, &self.app_conf.headers, &self.sink_conf.headers);
        builder = insert_query(
            builder,
            &self.app_conf.query_params,
            &self.sink_conf.query_params,
        );
        builder = insert_basic_auth(builder, &self.app_conf.basic_auth);

        let request = builder.build().unwrap();

        match self.http_client.execute(request).await {
            Ok(resp) => {
                if resp.status().is_success() {
                    let status_changed = self.error_manager.set_ok().await;
                    if status_changed {
                        self.app_err_tx.send(None).unwrap();
                    }
                } else {
                    let status_code = resp.status();
                    // TODO 是否会触发unwrap
                    let body = resp.text().await.unwrap();
                    let err = Arc::new(format!("状态码：{}, 错误：{}。", status_code, body));
                    self.error_manager.set_err(err.clone()).await;
                }
            }
            Err(e) => {
                let err = Arc::new(e.to_string());
                let status_changed = self.error_manager.set_err(err.clone()).await;
                if status_changed {
                    self.app_err_tx.send(Some(err.clone())).unwrap();
                }
            }
        }
    }
}

impl Sink {
    pub fn new(
        sink_id: String,
        app_conf: Arc<HttpClientConf>,
        sink_conf: SinkConf,
        app_err_tx: UnboundedSender<Option<Arc<String>>>,
    ) -> Sink {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = unbounded_channel();
        let (err1, err2) = BiLock::new(None);

        let task_loop = TaskLoop::new(
            sink_id,
            sink_conf,
            err1,
            stop_signal_rx,
            app_conf,
            app_err_tx,
            mb_rx,
        );
        let join_handle = task_loop.start();

        Sink {
            stop_signal_tx,
            mb_tx,
            err: err2,
            join_handle: Some(join_handle),
        }
    }

    pub fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub async fn update_conf(&mut self, _old_conf: SinkConf, new_conf: SinkConf) {
        let mut task_loop = self.stop().await;
        task_loop.sink_conf = new_conf;
        let join_handle = task_loop.start();
        self.join_handle = Some(join_handle);
    }

    pub async fn update_http_client(&mut self, app_conf: Arc<HttpClientConf>) {
        let mut task_loop = self.stop().await;
        task_loop.app_conf = app_conf;
        let join_handle = task_loop.start();
        self.join_handle = Some(join_handle);
    }
}
