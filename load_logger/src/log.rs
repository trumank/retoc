use anyhow::Result;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

pub fn setup_logging() -> Result<()> {
    let log_path = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .join("log.txt");
    let layer = fmt::layer()
        .with_writer(std::fs::File::create(log_path)?)
        .compact()
        .with_ansi(false)
        .with_level(true)
        .with_target(true)
        .without_time();

    tracing_subscriber::registry().with(layer).init();

    Ok(())
}
