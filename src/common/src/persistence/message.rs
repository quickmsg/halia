use anyhow::Result;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::PathBuf,
};

pub fn insert(path: PathBuf, content: &str, size: usize) -> Result<()> {
    let file = File::open(&path)?;
    let metadata = fs::metadata(&path)?;

    let reader = BufReader::new(file);
    // TODO path
    let mut tmp_file = OpenOptions::new().write(true).create(true).open(&path)?;

    let mut overflow_size = metadata.len() as usize + content.len() - size;
    for line in reader.lines() {
        let line = line?;
        if overflow_size > 0 {
            overflow_size -= line.len();
        } else {
            writeln!(tmp_file, "{line}")?;
        }
    }

    // TODO
    fs::rename(&path, &path)?;

    Ok(())
}
