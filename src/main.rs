use anyhow::{anyhow, Result};
use chrono::{NaiveDateTime, TimeZone, Utc};
use clap::Parser;
use std::fs;
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::str::FromStr;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::task::JoinSet;

/// Simple program to greet a person
#[derive(Parser, Debug)]
struct Args {
    /// Name of the person to greet
    task: String,
    path: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();

    match args.task.to_ascii_lowercase().as_str() {
        "datetimes" => {
            let path_str = args.path.as_str();
            match Path::new(path_str).is_dir() {
                true => date_time_csv_dir(path_str).await,
                false => date_time_csv_timestamps(path_str).await,
            }
        }
        _ => Err(anyhow!("undefined TASK")),
    }
}

async fn date_time_csv_dir(path_str: &str) -> Result<()> {
    let entries = fs::read_dir(path_str)?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, Error>>()?;

    let mut tasks: JoinSet<Result<()>> = JoinSet::new();

    for p in entries {
        tasks.spawn(async move {
            let is_file = p.is_file();
            if !is_file {
                return Ok(());
            }
            let Some(e) = p.extension() else {
                return Ok(());
            };
            let Some(p_str) = e.to_str() else {
                return Err(anyhow!("cant string?"));
            };
            if !p_str.eq_ignore_ascii_case("csv") {
                return Ok(());
            }
            let Some(file_str) = p.as_os_str().to_str() else {
                return Err(anyhow!("cant string?"));
            };
            date_time_csv_timestamps(file_str).await?;
            Ok(())
        });
    }

    while let Some(join_result) = tasks.join_next().await {
        let Ok(task_result) = join_result else {
            return Err(anyhow!(join_result.err().unwrap()));
        };
        let Ok(_) = task_result else {
            return Err(task_result.err().unwrap());
        };
    }

    Ok(())
}

async fn date_time_csv_timestamps(path: &str) -> Result<()> {
    let f = File::open(path).await?;
    let mut reader = BufReader::new(f);

    let Some(f_index) = path.rmatch_indices('/').map(|x| x.0).next() else {
        return Err(anyhow!("failed to split path"));
    };
    let split = path.split_at(f_index);
    let out_dir = Path::new(split.0).join("dt");
    if let Err(error) = fs::create_dir(&out_dir) {
        if !error.kind().eq(&ErrorKind::AlreadyExists) {
            return Err(anyhow!("can't create directory"));
        }
    }
    let out_path = format!("{}{}", out_dir.to_str().unwrap(), split.1);
    let out_file = File::create(out_path.as_str()).await?;
    let mut writer = BufWriter::new(out_file);

    let mut buffer = String::new();
    reader.read_line(&mut buffer).await?;
    // copy header
    writer.write_all(buffer.as_bytes()).await?;
    buffer.clear();
    loop {
        let len = reader.read_line(&mut buffer).await?;
        if len == 0 {
            break;
        }
        let Some(comma) = buffer.match_indices(',').map(|x| x.0).next() else {
            writer.write_all(buffer.as_bytes()).await?;
            buffer.clear();
            continue;
        };
        let split = buffer.split_at(comma);
        let timestamp = i64::from_str(split.0)?;
        if let Some(dt) = NaiveDateTime::from_timestamp_opt(timestamp, 0) {
            let dt = Utc.from_utc_datetime(&dt);
            let out = dt.to_rfc3339();
            writer.write_all(out.as_bytes()).await?;
            writer.write_all(split.1.as_bytes()).await?;
        } else {
            return Err(anyhow!("could not covert date time"));
        }
        buffer.clear();
    }
    writer.flush().await?;

    Ok(())
}
