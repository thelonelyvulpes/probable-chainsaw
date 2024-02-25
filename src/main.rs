mod date_times;
mod query_analyzer;

use anyhow::{anyhow, Result};
use clap::Parser;
use std::path::Path;

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
        }
        _ => Err(anyhow!("undefined TASK")),
    }
}
