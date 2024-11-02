use std::{io::SeekFrom, path::Path, time::Duration};

use anyhow::Result;
use bytes::BytesMut;
use notify::{Config, Event, EventHandler, RecommendedWatcher, RecursiveMode, Watcher as _};
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt as _, AsyncSeekExt as _},
    select,
    sync::{
        broadcast,
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch,
    },
};
use tracing::{debug, error};

struct EventHandlerImpl(UnboundedSender<Event>);

impl EventHandler for EventHandlerImpl {
    fn handle_event(&mut self, event: notify::Result<Event>) {
        if let Ok(event) = event {
            _ = self.0.send(event);
        }
    }
}

pub async fn tail_log_file(id: &String) -> Result<UnboundedReceiver<String>> {
    let path = format!("logs/{}.log", id);
    let mut file = OpenOptions::new().read(true).open(&path).await?;
    let size = file.metadata().await?.len();
    let mut buf = vec![0; size as usize];
    file.read_exact(&mut buf).await?;

    let (log_tx, log_rx) = unbounded_channel();
    log_tx.send(String::from_utf8(buf).unwrap())?;

    let (notify_tx, mut notify_rx) = unbounded_channel();
    // Automatically select the best implementation for your platform.
    // You can also access each implementation directly e.g. INotifyWatcher.
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
            match notify_rx.recv().await {
                Some(event) => match event.kind {
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
                },
                None => {
                    debug!("notify_rx closed");
                    return;
                }
            }
        }
    });

    Ok(log_rx)
}
