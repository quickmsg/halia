use std::{
    convert::Infallible,
    io::SeekFrom,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::Result;
use axum::{
    body::Body,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{sse, IntoResponse, Response, Sse},
};
use bytes::BytesMut;
use chrono::Local;
use futures::Stream;
use notify::{Config, Event, EventHandler, RecommendedWatcher, RecursiveMode, Watcher as _};
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _},
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch,
    },
    task::JoinHandle,
};
use tokio_util::io::ReaderStream;
use tracing::{debug, error};

fn get_log_filename(id: &String) -> String {
    format!("logs/{}.log", id)
}

struct EventHandlerImpl(UnboundedSender<Event>);

impl EventHandler for EventHandlerImpl {
    fn handle_event(&mut self, event: notify::Result<Event>) {
        if let Ok(event) = event {
            _ = self.0.send(event);
        }
    }
}

pub async fn tail_log(
    id: &String,
) -> Result<Sse<impl Stream<Item = Result<sse::Event, Infallible>>>> {
    let path = get_log_filename(id);
    let mut file = OpenOptions::new().read(true).open(&path).await?;
    let size = file.metadata().await?.len();
    let mut buf = vec![0; size as usize];
    file.read_exact(&mut buf).await?;

    let (log_tx, mut log_rx) = unbounded_channel();
    log_tx.send(String::from_utf8(buf).unwrap())?;

    let (notify_tx, mut notify_rx) = unbounded_channel();
    let mut watcher = RecommendedWatcher::new(
        EventHandlerImpl(notify_tx),
        Config::default()
            .with_poll_interval(Duration::from_secs(1))
            .with_compare_contents(false),
    )?;

    let mut start_pos = size;
    let mut buf = BytesMut::with_capacity(2048);
    tokio::spawn(async move {
        watcher
            .watch(Path::new(&path), RecursiveMode::NonRecursive)
            .unwrap();

        loop {
            select! {
                Some(event) = notify_rx.recv() => {
                    match event.kind {
                    notify::EventKind::Modify(modify_kind) => match modify_kind {
                        notify::event::ModifyKind::Data(data_change) => match data_change {
                            notify::event::DataChange::Any => {
                                file.seek(SeekFrom::Start(start_pos)).await.unwrap();
                                let read_byte = file.read_buf(&mut buf).await.unwrap();
                                if read_byte == 0 {
                                    continue;
                                }
                                start_pos += read_byte as u64;
                                if log_tx.is_closed() {
                                    error!("log_tx closed, quit tailing log");
                                    return;
                                }
                                debug!("{}", log_tx.is_closed());
                                if let Err(e) =
                                    log_tx.send(String::from_utf8_lossy(&buf).to_string())
                                {
                                    debug!("log_tx closed: {}", e);
                                }
                                buf.clear();
                            }
                            _ => {}
                        },
                        _ => {}
                    },
                    _ => {}
                }
                }

                _ = log_tx.closed() => {
                    debug!("log_tx closed, quit tailing log");
                    return;
                }
            }
        }
    });

    let stream = async_stream::stream! {
        loop {
            match log_rx.recv().await {
                Some(item) => {
                    debug!("::: {:?}", item);
                    debug!("here");
                    yield Ok(sse::Event::default().data(item));
                }
                None => {
                    debug!("quit");
                    break;
                }
            }
        }
        debug!("quit");
    };

    Ok(Sse::new(stream))
}

pub async fn download_log(id: &String) -> Response {
    let filename = get_log_filename(id);
    let file = match tokio::fs::File::open(filename).await {
        Ok(file) => file,
        Err(err) => {
            return (StatusCode::NOT_FOUND, format!("日志文件不存在: {}", err)).into_response()
        }
    };

    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"{}.log\"", id)).unwrap(),
    );
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );

    (headers, body).into_response()
}

pub struct Logger {
    enable: Arc<AtomicBool>,
    stop_signal: (watch::Sender<()>, Option<watch::Receiver<()>>),
    log_channel: (UnboundedSender<String>, Option<UnboundedReceiver<String>>),
    join_handle: Option<JoinHandle<(watch::Receiver<()>, UnboundedReceiver<String>)>>,
}

impl Logger {
    pub fn new() -> Self {
        let (logger_tx, logger_rx) = unbounded_channel();
        let (logger_stop_signal_tx, logger_stop_signal_rx) = watch::channel(());
        Self {
            enable: Arc::new(AtomicBool::new(false)),
            stop_signal: (logger_stop_signal_tx, Some(logger_stop_signal_rx)),
            log_channel: (logger_tx, Some(logger_rx)),
            join_handle: None,
        }
    }

    pub async fn start(&mut self, id: &String) {
        if self.enable.swap(true, std::sync::atomic::Ordering::Relaxed) {
            return;
        }

        let mut log_rx = self.log_channel.1.take().unwrap();
        let mut stop_signal_rx = self.stop_signal.1.take().unwrap();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!("logs/{}.log", id))
            // TODO remove unwrap
            .await
            .unwrap();
        let join_handle = tokio::spawn(async move {
            loop {
                select! {
                    Some(log) = log_rx.recv() => {
                        file.write(format!("{}:      {}\n", Local::now(), log).as_bytes()).await.unwrap();
                        file.flush().await.unwrap();
                    }
                    _ = stop_signal_rx.changed() => {
                        debug!("logger stop");
                        file.flush().await.unwrap();
                        return (stop_signal_rx, log_rx);
                    }
                }
            }
        });
        self.join_handle = Some(join_handle);
    }

    pub async fn stop(&mut self) {
        if !self
            .enable
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }

        self.stop_signal.0.send(()).unwrap();
        let join_handle = self.join_handle.take().unwrap().await.unwrap();
        self.stop_signal.1 = Some(join_handle.0);
        self.log_channel.1 = Some(join_handle.1);
    }

    pub fn get_logger_item(&self) -> LoggerItem {
        LoggerItem {
            enable: self.enable.clone(),
            tx: self.log_channel.0.clone(),
        }
    }
}

#[derive(Clone)]
pub struct LoggerItem {
    enable: Arc<AtomicBool>,
    tx: UnboundedSender<String>,
}

impl LoggerItem {
    pub fn is_enable(&self) -> bool {
        self.enable.load(Ordering::Relaxed)
    }

    pub fn log(&self, log: String) {
        // 有可能channel已关闭
        _ = self.tx.send(log);
    }
}
