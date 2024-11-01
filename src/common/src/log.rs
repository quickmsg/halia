use std::{io::SeekFrom, path::Path, sync::mpsc::channel};

use anyhow::Result;
use bytes::BytesMut;
use notify::{RecursiveMode, Watcher as _};
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt as _, AsyncSeekExt as _},
    sync::mpsc::{unbounded_channel, UnboundedReceiver},
};
use tracing::error;

pub async fn tail_log_file(id: &String) -> Result<UnboundedReceiver<String>> {
    let path = format!("logs/{}.log", id);

    let mut file = OpenOptions::new().read(true).open(&path).await?;
    let size = file.metadata().await?.len();
    let mut buf = vec![0; size as usize];
    file.read_exact(&mut buf).await?;

    let (tx, rx) = unbounded_channel();

    tx.send(String::from_utf8(buf).unwrap())?;

    let (notify_tx, notify_rx) = channel();
    let mut watcher = notify::recommended_watcher(notify_tx)?;
    watcher.watch(Path::new(&path), RecursiveMode::NonRecursive)?;

    let mut start_pos = size;
    let mut buf = BytesMut::with_capacity(1024);

    tokio::spawn(async move {
        loop {
            match notify_rx.recv() {
                Ok(event) => match event {
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
                                    tx.send(String::from_utf8_lossy(&buf).to_string()).unwrap();
                                    buf.clear();
                                }
                                _ => {}
                            },
                            _ => {}
                        },
                        _ => {}
                    },
                    Err(e) => println!("watch error: {:?}", e),
                },
                Err(e) => error!("watch error here: {:?}", e),
            }
        }
    });

    Ok(rx)
}
