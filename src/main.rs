use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::{error, info, warn};
use tokio::sync::watch;

mod discord;
mod lab;
mod storage;

type FailureOr<T> = Result<T, failure::Error>;

type BuildNumber = u32;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum BuilderState {
    Building,
    Idle,
    Offline,
}

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

#[derive(Clone, Debug)]
struct BotStatus {
    // If the most_recent_build is red, this'll be the first build we know of that failed.
    first_failing_build: Option<CompletedBuild>,
    most_recent_build: CompletedBuild,
    state: BuilderState,
}

#[derive(Clone, Debug)]
struct Bot {
    category: String,
    status: BotStatus,
}

// !! FIXME: greendragon notes !!
// fhahn notes
// """
// @gburgessiv It looks like green.lab.llvm.org has a REST API link at the bottom of the pages for
// the bots (and the main index), like
// http://green.lab.llvm.org/green/job/clang-stage1-cmake-RA-incremental/api/ which describes the
// endpoints :slight_smile:
// """
//
// http://green.lab.llvm.org/green/api/json is probably the main endpoint; it has things like
// clang-stage1-cmake-RA-expensive.
//
// An example of a single builder's recent builds is:
// http://green.lab.llvm.org/green/job/clang-stage1-cmake-RA-incremental/api/json?pretty=true
//
// http://green.lab.llvm.org/green/job/clang-stage1-cmake-RA-incremental/8387/api/json?pretty=true
// is a single build (so just add /api/json?pretty=true to a link)
//
// OK. So `changeSets` is left empty if it's an incomplete build, and populated if it's complete.
// That's something to go off of, at least. They also have `authorEmail` fields, so we can pretty
// easily turn that into a blamelist. Notably, authorEmail excludes the name of the author.
//
// authorEmail is once per commit in changeSets. No author name is there. If we want to map to
// author name, we have commitId; we can maintain a llvm-project checkout and go from there, but
// that's probably unnecessary.
//
// RE the build status type, we have a 'result'; "FAILURE" is one value for this. Need to figure
// that out more deeply.
//
// In any case, the question I have at this point is "if we were to start fresh, how would I change
// what's here?"
//
// BotStatusSnapshot is probably still appropriate, as is Bot (GreenDragon can maybe be the
// category? Just have to hope the two don't collide.)
// BotStatus works.
// state works.
// completedbuild works.
//
// cool. so just make them not collide by adding a `master` bit to names.
//
// I have lotsa URLs to tweak though. Let's start with that.

#[derive(Clone, Copy, Eq, PartialEq, Debug, Hash, Ord, PartialOrd)]
enum Master {
    Lab,
    GreenDragon,
}

#[derive(Clone, PartialEq, Eq, Ord, PartialOrd, Hash, Debug)]
struct BotID {
    master: Master,
    name: String,
}

#[derive(Clone, Debug, Default)]
struct BotStatusSnapshot {
    bots: HashMap<BotID, Bot>,
}

fn new_async_ticker(period: Duration) -> tokio::sync::mpsc::Receiver<()> {
    let (mut tx, rx) = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
        let mut start = Instant::now();
        while let Ok(_) = tx.send(()).await {
            let ideal_next = start + period;
            let now = Instant::now();

            start = if now < ideal_next {
                tokio::timer::delay(ideal_next).await;
                ideal_next
            } else {
                now
            };
        }
    });
    rx
}

async fn publish_forever(
    client: lab::Client,
    notifications: watch::Sender<Option<Arc<BotStatusSnapshot>>>,
) {
    let mut ticks = new_async_ticker(Duration::from_secs(60));
    let mut last_snapshot = Arc::new(BotStatusSnapshot::default());
    loop {
        ticks.recv().await.expect("ticker shut down unexpectedly");
        match lab::fetch_new_status_snapshot(&client, &last_snapshot).await {
            Ok(snapshot) => {
                last_snapshot = Arc::new(BotStatusSnapshot { bots: snapshot });
                info!(
                    "Snapshot with {} bots ({} success / {} failures) updated successfully.",
                    last_snapshot.bots.len(),
                    last_snapshot
                        .bots
                        .values()
                        .filter(|x| x.status.first_failing_build.is_none())
                        .count(),
                    last_snapshot
                        .bots
                        .values()
                        .filter(|x| x.status.first_failing_build.is_some())
                        .count(),
                );

                if notifications
                    .broadcast(Some(last_snapshot.clone()))
                    .is_err()
                {
                    warn!("All lab handles are closed; shutting down publishing loop");
                }
            }
            Err(x) => {
                error!("Updating bot statuses failed: {}\n{}", x, x.backtrace());
            }
        };
    }
}

fn main() -> FailureOr<()> {
    simple_logger::init_with_level(log::Level::Info).unwrap();
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
    let client = lab::Client::new("http://lab.llvm.org:8011")?;
    let storage = storage::Storage::from_file(&database_file)?;
    let tokio_rt = tokio::runtime::Runtime::new()?;
    let (snapshots_tx, snapshots_rx) = watch::channel(None);

    tokio_rt.spawn(publish_forever(client, snapshots_tx));
    discord::run(
        &discord_token,
        git_version::git_version!(),
        snapshots_rx,
        tokio_rt.executor(),
        storage,
    )
}
