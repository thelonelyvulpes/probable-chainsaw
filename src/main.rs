mod date_times;
mod query_analyzer;

use anyhow::{anyhow, Result};
use clap::Parser;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use regex::{Regex, RegexBuilder};

#[derive(Parser, Debug)]
struct Args {
    task: String,
    path: String,
    #[arg(short, default_value = "")]
    regex: String,
    #[arg(short, default_value = "")]
    outfile: String
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();

    match args.task.to_ascii_lowercase().as_str() {
        "datetimes" => {
            let path_str = args.path.as_str();
            let path = Path::new(path_str);
            match path.is_dir() {
                true => date_times::date_time_csv_dir(path_str).await,
                false => date_times::date_time_csv_timestamps(path_str).await,
            }
        }
        "querylog" => {
            let path_str = args.path.as_str();
            let path = Path::new(path_str);
            match path.is_dir() {
                true => query_analyzer::analyze_dir(path_str).await,
                false => Err(anyhow!("not a directory")),
            }
        },
        "keep" => {
            let path_str = args.path.as_str();
            let path = Path::new(path_str);
            let f = File::open(path).await?;
            let mut reader = BufReader::new(f);
            let stringoutfile = args.outfile;
            let outpath = stringoutfile.as_str();
            let out_file = File::create(outpath).await?;
            let mut writer = BufWriter::new(out_file);
            let regex_string = args.regex;
            let re_str = regex_string.as_str();
            let re = RegexBuilder::new(re_str);
            let regex = re.build().unwrap();

            let mut buffer = String::new();
            loop {
                let len = reader.read_line(&mut buffer).await?;
                if len == 0 {
                    break;
                }

                if regex.is_match(buffer.as_str()) {
                    writer.write_all(buffer.as_bytes()).await?;
                }
                buffer.clear();
            }
            writer.flush().await?;

            Ok(())
        }
        _ => Err(anyhow!("undefined TASK")),
    }
}
