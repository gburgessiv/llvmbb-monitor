use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;

#[derive(Parser)]
struct Args {
    #[clap(long)]
    debug: bool,
}

async fn run(args: Args) -> Result<()> {
    stderrlog::new()
        .verbosity(if args.debug {
            log::Level::Debug
        } else {
            log::Level::Info
        })
        .timestamp(stderrlog::Timestamp::Millisecond)
        .init()
        .unwrap();

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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    run(args).await
}
