use anyhow::Result;
use std::collections::HashMap;
use std::fs;
use std::io::Error;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

pub(crate) async fn analyze_dir(path_str: &str) -> Result<()> {
    let entries = fs::read_dir(path_str)?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, Error>>()?;

    let mut data = HashMap::new();

    for entry in entries {
        process(&mut data, &entry).await?;
    }

    println!("{:?}", data);
    Ok(())
}

async fn process(p0: &mut HashMap<&str, f64>, p1: &PathBuf) -> Result<()> {
    sleep(Duration::from_millis(1)).await;

    if !p0.contains_key("hello") {
        p0.insert("hello", 0f64);
        return Ok(());
    }

    let x = p0.get_mut("hello");
    match x {
        None => {}
        Some(v) => {
            *v = *v + 1f64;
        }
    }
    Ok(())
}
