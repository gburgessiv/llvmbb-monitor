#![feature(is_sorted)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use log::{error, info, warn};
use tokio::sync::watch;

mod discord;
mod greendragon;
mod lab;
mod storage;

// A number used to identify a build. Note that this isn't expected to identify a single build for
// a single source (e.g., lab or green dragon): the ones we get from lab uniquely identify a build
// _per builder_, but multiple builders can totally have overlapping BuildNumbers.
type BuildNumber = u32;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum BuildbotResult {
    Success,
    Warnings,
    Failure,
    // Generally only present in individual steps.
    Skipped,
    Exception,
}

#[derive(Hash, Eq, PartialEq, Ord, PartialOrd, Clone, Debug)]
struct Email {
    address: Box<str>,
    at_loc: usize,
}

impl Email {
    fn parse(from: &str) -> Option<Email> {
        from.find('@').map(|x| Email {
            // FIXME: to_lowercase here is a bit of a hack.
            address: from.to_lowercase().into_boxed_str(),
            at_loc: x,
        })
    }

    fn account_with_plus(&self) -> &str {
        &self.address[..self.at_loc]
    }

    fn domain(&self) -> &str {
        &self.address[self.at_loc + 1..]
    }

    fn address(&self) -> &str {
        &self.address
    }
}

#[derive(Clone, Debug)]
struct CompletedBuild {
    id: BuildNumber,
    status: BuildbotResult,
    completion_time: chrono::NaiveDateTime,
    // This is 'blame' in the same way that 'git blame' is 'blame': it's the set of authors who
    // have changes in the current build.
    blamelist: Vec<Email>,
}

// FIXME: Adding a 'certain' field and surfacing that in discord may be nice? If a bot history goes
// back up to 7 weeks, we're doomed to always report 7wks + $(uptiem) of brokenness for the bot.
// Would be nice to put a '>' there.
#[derive(Clone, Debug)]
struct BotStatus {
    // If the most_recent_build is red, this'll be the first build we know of that failed.
    first_failing_build: Option<CompletedBuild>,
    most_recent_build: CompletedBuild,
    is_online: bool,
}

#[derive(Clone, Debug)]
struct Bot {
    category: String,
    status: BotStatus,
}

#[derive(Clone, PartialEq, Eq, Ord, PartialOrd, Hash, Debug)]
enum BotID {
    Lab { id: lab::BotID, name: String },
    GreenDragon { name: String },
}

#[derive(Clone, Debug, Default)]
struct BotStatusSnapshot {
    bots: HashMap<BotID, Bot>,
}

async fn publish_forever(
    client: reqwest::Client,
    notifications: watch::Sender<Option<Arc<BotStatusSnapshot>>>,
) {
    let mut ticks = tokio::time::interval(Duration::from_secs(60));
    let mut lab_state = None;
    let mut last_lab_snapshot = HashMap::new();
    let mut last_greendragon_snapshot = HashMap::new();
    loop {
        ticks.tick().await;

        let start_time = Instant::now();
        let (lab_update, greendragon_update) = futures::future::join(
            lab::fetch_new_status_snapshot(&client, &mut lab_state, &last_lab_snapshot),
            greendragon::fetch_new_status_snapshot(&client, &last_greendragon_snapshot),
        )
        .await;

        let mut update_success = false;
        match lab_update {
            Ok(snapshot) => {
                last_lab_snapshot = snapshot;
                update_success = true;
            }
            Err(x) => {
                error!("Updating lab bot statuses failed:\n{:?}", x);
            }
        }

        match greendragon_update {
            Ok(snapshot) => {
                last_greendragon_snapshot = snapshot;
                update_success = true;
            }
            Err(x) => {
                error!("Updating greendragon bot statuses failed:\n{:?}", x);
            }
        };

        if !update_success {
            continue;
        }

        let this_snapshot: HashMap<BotID, Bot> = last_lab_snapshot
            .iter()
            .map(|(id, (name, bot))| {
                (
                    BotID::Lab {
                        id: *id,
                        name: name.clone(),
                    },
                    bot.clone(),
                )
            })
            .chain(
                last_greendragon_snapshot
                    .iter()
                    .map(|(name, bot)| (BotID::GreenDragon { name: name.clone() }, bot.clone())),
            )
            .collect();

        let elapsed_time = start_time.elapsed();
        info!(
            "Full snapshot of {} bots ({} success / {} failures) updated successfully in {}.{:03}s.",
            this_snapshot.len(),
            this_snapshot
                .values()
                .filter(|x| x.status.first_failing_build.is_none())
                .count(),
            this_snapshot
                .values()
                .filter(|x| x.status.first_failing_build.is_some())
                .count(),
            elapsed_time.as_secs(),
            elapsed_time.subsec_millis(),
        );

        let this_snapshot = Arc::new(BotStatusSnapshot {
            bots: this_snapshot,
        });
        if notifications.send(Some(this_snapshot)).is_err() {
            warn!("All lab handles are closed; shutting down publishing loop");
            return;
        }
    }
}

fn init_logger_or_die() {
    let mut logger = simple_logger::SimpleLogger::new().with_level(log::LevelFilter::Warn);
    // Hyper and reqwest give a loot of `DEBUG` information.
    logger = logger.with_module_level(
        "llvm_buildbot_monitor",
        if cfg!(debug_assertions) {
            log::LevelFilter::Debug
        } else {
            log::LevelFilter::Info
        },
    );
    logger.init().unwrap();
}

fn main() -> Result<()> {
    init_logger_or_die();
    let matches = clap::App::new("llvm_buildbot_monitor")
        .arg(
            clap::Arg::with_name("discord_token")
                .long("discord_token")
                .takes_value(true)
                .required(true),
        )
        .arg(
            clap::Arg::with_name("database")
                .long("database")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    let discord_token = matches.value_of("discord_token").unwrap();
    let database_file = matches.value_of("database").unwrap();
    let client = reqwest::ClientBuilder::new()
        // The lab can take a while to hand back results, but 60 seconds should be enough
        // for anybody.
        .timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(lab::MAX_CONCURRENCY)
        .build()?;
    let storage = storage::Storage::from_file(&database_file)?;
    let tokio_rt = tokio::runtime::Runtime::new()?;
    let (snapshots_tx, snapshots_rx) = watch::channel(None);

    tokio_rt.spawn(publish_forever(client, snapshots_tx));
    discord::run(
        &discord_token,
        git_version::git_version!(),
        snapshots_rx,
        tokio_rt,
        storage,
    )
}
