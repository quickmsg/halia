use std::{io::SeekFrom, path::Path, time::Duration};

use anyhow::Result;
use bytes::BytesMut;
use notify::{Config, RecommendedWatcher, RecursiveMode, Watcher as _};
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt as _, AsyncSeekExt as _},
    select,
    sync::{
        broadcast,
        mpsc::{unbounded_channel, UnboundedReceiver},
    },
};
use tracing::{debug, error};

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
        move |res| {
            debug!("event: {:?}", res);
            notify_tx.send(res).unwrap();
        },
        Config::default()
            .with_poll_interval(Duration::from_secs(2))
            .with_compare_contents(false),
    )?;
    watcher.watch(Path::new(&path), RecursiveMode::NonRecursive)?;

    let mut start_pos = size;
    let mut buf = BytesMut::with_capacity(2048);
    let (stop_signal_tx, mut stop_signal_rx) = broadcast::channel::<()>(1);

    tokio::spawn(async move {
        loop {
            select! {
                Some(res) = notify_rx.recv() => {
                    debug!("watch");
                    match res {
                        Ok(event) => match event.kind {
                            notify::EventKind::Modify(modify_kind) => match modify_kind {
                                notify::event::ModifyKind::Data(data_change) => match data_change {
                                    notify::event::DataChange::Any => {
                                        file.seek(SeekFrom::Start(start_pos)).await.unwrap();
                                        let read_byte = file.read_buf(&mut buf).await.unwrap();
                                        if read_byte == 0 {
                                            continue;
                                        }
                                        start_pos += read_byte as u64;
                                        log_tx
                                            .send(String::from_utf8_lossy(&buf).to_string())
                                            .unwrap();
                                        debug!("send");
                                        buf.clear();
                                    }
                                    notify::event::DataChange::Size => {
                                        debug!("file size changed");
                                    }
                                    notify::event::DataChange::Content => {
                                        debug!("file content changed");
                                    }
                                    notify::event::DataChange::Other => {
                                        debug!("file other changed");
                                    }
                                },
                                notify::event::ModifyKind::Any => {
                                    debug!("any");
                                }
                                notify::event::ModifyKind::Metadata(metadata_kind) => {
                                    debug!("metadata");
                                }
                                notify::event::ModifyKind::Name(rename_mode) => {
                                    debug!("rename");
                                }
                                notify::event::ModifyKind::Other => {
                                    debug!("other");
                                }
                            },
                            notify::EventKind::Any => {
                                debug!("any");
                            }
                            notify::EventKind::Access(access_kind) => {
                                debug!("access");
                            }
                            notify::EventKind::Create(create_kind) => {
                                debug!("create");
                            }
                            notify::EventKind::Remove(remove_kind) => {
                                debug!("remove");
                            }
                            notify::EventKind::Other => {
                                debug!("other");
                            }
                        },
                        Err(e) => error!("watch error here: {:?}", e),
                    }
                }
                _ = stop_signal_rx.recv() => {
                    debug!("log quit.");
                    return
                }
            }
        }
        // debug!("here");
        // while let Some(res) = notify_rx.recv().await {
        //     debug!("watch");
        //     match res {
        //         Ok(event) => match event.kind {
        //             notify::EventKind::Modify(modify_kind) => match modify_kind {
        //                 notify::event::ModifyKind::Data(data_change) => match data_change {
        //                     notify::event::DataChange::Any => {
        //                         file.seek(SeekFrom::Start(start_pos)).await.unwrap();
        //                         let read_byte = file.read_buf(&mut buf).await.unwrap();
        //                         if read_byte == 0 {
        //                             continue;
        //                         }
        //                         start_pos += read_byte as u64;
        //                         log_tx
        //                             .send(String::from_utf8_lossy(&buf).to_string())
        //                             .unwrap();
        //                         debug!("send");
        //                         buf.clear();
        //                     }
        //                     notify::event::DataChange::Size => {
        //                         debug!("file size changed");
        //                     }
        //                     notify::event::DataChange::Content => {
        //                         debug!("file content changed");
        //                     }
        //                     notify::event::DataChange::Other => {
        //                         debug!("file other changed");
        //                     }
        //                 },
        //                 notify::event::ModifyKind::Any => {
        //                     debug!("any");
        //                 }
        //                 notify::event::ModifyKind::Metadata(metadata_kind) => {
        //                     debug!("metadata");
        //                 }
        //                 notify::event::ModifyKind::Name(rename_mode) => {
        //                     debug!("rename");
        //                 }
        //                 notify::event::ModifyKind::Other => {
        //                     debug!("other");
        //                 }
        //             },
        //             notify::EventKind::Any => {
        //                 debug!("any");
        //             }
        //             notify::EventKind::Access(access_kind) => {
        //                 debug!("access");
        //             }
        //             notify::EventKind::Create(create_kind) => {
        //                 debug!("create");
        //             }
        //             notify::EventKind::Remove(remove_kind) => {
        //                 debug!("remove");
        //             }
        //             notify::EventKind::Other => {
        //                 debug!("other");
        //             }
        //         },
        //         Err(e) => error!("watch error here: {:?}", e),
        //     }
        // }

        debug!("log quit.");
    });

    Ok(log_rx)
}
