use std::{io::SeekFrom, path::Path, sync::mpsc::channel};

const BUF_SIZE: usize = 1024;

use anyhow::Result;
use notify::{RecursiveMode, Watcher as _};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncSeekExt as _, BufReader},
};
use tracing::warn;

pub async fn read_log() -> Result<()> {
    let path = "src/common/src/log.rs";
    let mut file = File::open(path).await?;
    // let reader = BufReader::new(file);
    // // 逐行读取文件内容
    // for line in reader.lines() {
    //     let line = line?; // 处理 Result，获取行内容
    //     println!("{}", line); // 打印行内容
    // }

    let contents = fs::read_to_string(path).await?;
    let (tx, rx) = channel();
    let mut watcher = notify::recommended_watcher(tx)?;
    watcher.watch(Path::new("."), RecursiveMode::Recursive)?;

    loop {
        let a = rx.recv().unwrap();
        match a {
            Ok(event) => match event.kind {
                notify::EventKind::Modify(mk) => {
                    match mk {
                        notify::event::ModifyKind::Any => todo!(),
                        notify::event::ModifyKind::Data(_) => {
                            todo!()
                        }
                        notify::event::ModifyKind::Metadata(metadata_kind) => todo!(),
                        notify::event::ModifyKind::Name(rename_mode) => todo!(),
                        notify::event::ModifyKind::Other => todo!(),
                    }
                    let contents = fs::read_to_string(path).await?;
                    println!("file modified: {:?}", contents);
                }
                notify::EventKind::Any => todo!(),
                notify::EventKind::Access(access_kind) => todo!(),
                notify::EventKind::Create(create_kind) => todo!(),
                notify::EventKind::Remove(remove_kind) => todo!(),
                notify::EventKind::Other => todo!(),
            },
            Err(_) => todo!(),
        }
    }

    for res in rx {
        match res {
            Ok(event) => println!("event: {:?}", event),
            Err(e) => println!("watch error: {:?}", e),
        }
    }

    todo!()
}

async fn tail_file(path: &String, count: u64, fflag: bool) {
    //let file = match File::open(path){
    let file = match OpenOptions::new().read(true).open(path).await {
        Ok(file) => file,
        Err(e) => {
            warn!("Cannot open file! file:{} cause:{}", path, e);
            return;
        } // Err (why) => panic!("Cannot open file! file:{} cause:{}", path, Error::description(&why)),
          // Ok(file) => file
    };
    let md = match file.metadata().await {
        Ok(md) => md,
        Err(_) => todo!(),
    };
    let size = md.len();
    //println!("file size is {} bytes", f_size);
    let mut reader = BufReader::new(file);

    let mut line_count = 0;
    // minus 2 byte for skip eof null byte.
    let mut current_pos = size - 2;
    let mut read_start = if (size - 2) > BUF_SIZE as u64 {
        size - 2 - BUF_SIZE as u64
    } else {
        0
    };
    let mut buf = [0; BUF_SIZE];
    'outer: loop {
        match reader.seek(SeekFrom::Start(read_start)).await {
            Ok(_) => current_pos,
            Err(_) => todo!(),
            // Err(why) => panic!("Cannot move offset! offset:{} cause:{}", current_pos, Error::description(&why)),
            // Ok(_) => current_pos
        };
        let b = match reader.read(&mut buf).await {
            Ok(b) => b,
            Err(_) => todo!(),
            // Err(why) => panic!("Cannot read offset byte! offset:{} cause:{}", current_pos, Error::description(&why)),
            // Ok(b) => b
        };
        for i in 0..b {
            if buf[b - (i + 1)] == 0xA {
                line_count += 1;
            }
            // println!("{}, {}", line_count, i);
            if line_count == count {
                break 'outer;
            }
            current_pos -= 1;
            //println!("{}", current_pos);
            if current_pos <= 0 {
                current_pos = 0;
                break 'outer;
            }
        }
        read_start = if read_start > BUF_SIZE as u64 {
            read_start - BUF_SIZE as u64
        } else {
            0
        }
    }
    //println!("last pos :{}", current_pos);
    match reader.seek(SeekFrom::Start(current_pos)).await {
        Ok(_) => current_pos,
        Err(_) => todo!(),
        // Err(why) => panic!("Cannot read offset byte! offset:{} cause:{}", current_pos, Error::description(&why)),
        // Ok(_) => current_pos
    };
    let mut buf_str = String::new();
    match reader.read_to_string(&mut buf_str).await {
        Ok(_) => current_pos,
        Err(_) => todo!(),
        // Err(why) => panic!("Cannot read offset byte! offset:{} cause:{}", current_pos, Error::description(&why)),
        // Ok(_) => current_pos
    };
    print_result(buf_str);
    if let Err(e) = tail_file_follow(&mut reader, path, size).await {
        warn!("Cannot follow file! file:{} cause:{}", path, e);
    }
}

async fn tail_file_follow(
    reader: &mut BufReader<File>,
    spath: &String,
    file_size: u64,
) -> notify::Result<()> {
    let (tx, rx) = channel();
    let mut watcher = notify::recommended_watcher(tx)?;
    watcher.watch(Path::new("."), RecursiveMode::NonRecursive)?;

    let mut start_byte = file_size;
    let mut buf_str = String::new();
    loop {
        match rx.recv() {
            Err(e) => println!("watch error: {:?}", e),
            Ok(_) => {
                match reader.seek(SeekFrom::Start(start_byte)).await {
                    Err(_) => panic!("Cannot move offset! offset: cause:",),
                    Ok(_) => start_byte,
                };
                let read_byte = match reader.read_to_string(&mut buf_str).await {
                    Err(why) => panic!("Cannot read offset byte! offset: cause:",),
                    Ok(b) => b,
                };
                start_byte += read_byte as u64;
                print_result(buf_str.clone());
                buf_str.clear();
            }
        }
    }
}

fn print_result(disp_str: String) {
    print!("{}", disp_str);
}
