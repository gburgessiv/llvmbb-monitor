use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;

#[derive(Parser)]
struct Args {
    #[clap(long)]
    debug: bool,
}

async fn run() -> Result<()> {
    let events = calendar_check::fetch_near_llvm_calendar_office_hour_events(
        &reqwest::Client::new(),
        &chrono::Utc::now(),
        // Fetch a few weeks out. Doubt anyone will care about more than that.
        Duration::from_secs(14 * 24 * 60 * 60),
    )
    .await
    .context("fetching LLVM office hour events")?;

    println!("{:#?}", events);
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    simple_logger::init_with_level(if args.debug {
        log::Level::Debug
    } else {
        log::Level::Info
    })
    .unwrap();

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?
        .block_on(run())
}
