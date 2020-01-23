use std::collections::{HashMap, HashSet};
use std::fmt;
use std::iter::Fuse;
use std::sync::Arc;
use std::time::{Duration, Instant};

use failure::bail;
use log::{debug, error, info, warn};
use serde::Deserialize;
use tokio::sync::watch;

mod discord;
mod storage;

type FailureOr<T> = Result<T, failure::Error>;

type BuildNumber = u32;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum BuilderState {
    Building,
    Idle,
    Offline,
}

impl BuilderState {
    fn valid_values() -> &'static [(&'static str, Self)] {
        &[
            ("building", Self::Building),
            ("idle", Self::Idle),
            ("offline", Self::Offline),
        ]
    }
}

struct BuilderStateVisitor;

impl<'de> serde::de::Visitor<'de> for BuilderStateVisitor {
    type Value = BuilderState;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "one of {:?}",
            Self::Value::valid_values()
                .iter()
                .map(|x| x.0)
                .collect::<Vec<&'static str>>()
        )
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        for (name, res) in Self::Value::valid_values() {
            if *name == s {
                return Ok(*res);
            }
        }

        Err(E::custom(format!("{:?} isn't a valid builder state", s)))
    }
}

impl<'de> Deserialize<'de> for BuilderState {
    fn deserialize<D>(deserializer: D) -> Result<BuilderState, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_str(BuilderStateVisitor)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UnabridgedBuilderStatus {
    category: String,
    cached_builds: Vec<BuildNumber>,
    #[serde(default)]
    current_builds: Vec<BuildNumber>,
    state: BuilderState,
}

impl UnabridgedBuilderStatus {
    fn completed_builds_ascending(&self) -> Vec<BuildNumber> {
        let running: HashSet<BuildNumber> = self.current_builds.iter().copied().collect();
        let mut result: Vec<BuildNumber> = self
            .cached_builds
            .iter()
            .copied()
            .filter(|x| !running.contains(x))
            .collect();
        result.sort();
        result
    }
}

#[derive(Copy, Clone, Debug, Default, Deserialize)]
#[serde(transparent)]
struct RawBuildbotResult(i64);

impl RawBuildbotResult {
    fn as_buildbot_result(self) -> FailureOr<BuildbotResult> {
        match self.0 {
            0 => Ok(BuildbotResult::Success),
            1 => Ok(BuildbotResult::Warnings),
            2 => Ok(BuildbotResult::Failure),
            3 => Ok(BuildbotResult::Skipped),
            4 => Ok(BuildbotResult::Exception),
            // 5 is technically 'RETRY'. For our purposes, that's an exception.
            5 => Ok(BuildbotResult::Exception),
            n => bail!("{} is an invalid buildbot result", n),
        }
    }
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

#[derive(Copy, Clone, Debug, Deserialize)]
#[serde(transparent)]
struct RawBuildbotTime(f64);

impl RawBuildbotTime {
    fn as_datetime(self) -> FailureOr<chrono::NaiveDateTime> {
        let secs = self.0 as i64;
        let nanos = ((self.0 - secs as f64) * 1_000_000_000f64) as u32;
        match chrono::NaiveDateTime::from_timestamp_opt(secs, nanos) {
            Some(x) => Ok(x),
            None => bail!("invalid timestamp: {}", self.0),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UnabridgedBuildStatus {
    blame: Vec<String>,
    builder_name: String,
    number: BuildNumber,
    #[serde(default)]
    results: RawBuildbotResult,
    times: (RawBuildbotTime, Option<RawBuildbotTime>),
}

// "Foo Bar <foo@bar.com>" => "foo@bar.com"
// Otherwise, returns the original email unaltered.
fn remove_name_from_email(email: &str) -> &str {
    match (email.rfind('<'), email.rfind('>')) {
        (Some(x), Some(y)) if x < y => &email[x + 1..y],
        _ => email,
    }
}

impl UnabridgedBuildStatus {
    fn into_completed_build(self) -> FailureOr<CompletedBuild> {
        let completion_time: chrono::NaiveDateTime = match self.times.1 {
            Some(x) => x.as_datetime()?,
            None => bail!("build has not yet completed"),
        };

        let mut blamelist: Vec<Email> = Vec::with_capacity(self.blame.len());
        for email in self.blame {
            let email = remove_name_from_email(&email);
            match Email::parse(email) {
                Some(x) => blamelist.push(x),
                None => warn!(
                    "Failed parsing email {} for {}/{} (completed at {})",
                    email, self.builder_name, self.number, completion_time
                ),
            }
        }

        blamelist.sort();
        Ok(CompletedBuild {
            id: self.number,
            status: self.results.as_buildbot_result()?,
            completion_time,
            blamelist,
        })
    }
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

struct LLVMLabClient {
    client: reqwest::Client,
    host: reqwest::Url,
}

impl LLVMLabClient {
    // host is e.g., http://lab.llvm.org:8011
    fn new(host: &str) -> FailureOr<Self> {
        Ok(Self {
            client: reqwest::ClientBuilder::new()
                // The lab can take a while to hand back results, but 60 minutes should be enough
                // for anybody.
                .timeout(Duration::from_secs(60))
                // Don't slam lab.llvm.org.
                .max_idle_per_host(3)
                .build()?,
            host: reqwest::Url::parse(host)?,
        })
    }

    async fn json_get<T>(&self, path: &str) -> FailureOr<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let resp = self.client.get(self.host.join(path)?).send().await?;
        Ok(resp.json().await?)
    }

    async fn fetch_full_builder_status(
        &self,
    ) -> FailureOr<HashMap<String, UnabridgedBuilderStatus>> {
        Ok(self.json_get("/json/builders").await?)
    }

    async fn fetch_build_statuses(
        &self,
        builder: &str,
        numbers: &[BuildNumber],
    ) -> FailureOr<Vec<UnabridgedBuildStatus>> {
        if numbers.is_empty() {
            return Ok(Vec::new());
        }

        let mut url = format!("/json/builders/{}/builds?", builder);
        use std::fmt::Write;
        for (i, n) in numbers.iter().enumerate() {
            if i != 0 {
                url.push('&');
            }
            write!(url, "select={}", n).unwrap();
        }

        let mut results: HashMap<String, UnabridgedBuildStatus> = self.json_get(&url).await?;
        let mut ordered_results: Vec<UnabridgedBuildStatus> = Vec::with_capacity(numbers.len());
        for number in numbers {
            match results.remove(&format!("{}", number)) {
                Some(x) => ordered_results.push(x),
                None => {
                    let unique_numbers: HashSet<BuildNumber> = numbers.iter().copied().collect();
                    if unique_numbers.len() != numbers.len() {
                        bail!("duplicate build numbers requested: {:?}", numbers);
                    }
                    bail!("endpoint failed to return a build for {}", number);
                }
            }
        }

        Ok(ordered_results)
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

struct BuildStream<'a, Iter>
where
    Iter: Iterator<Item = BuildNumber>,
{
    builder_name: &'a str,
    client: &'a LLVMLabClient,
    ids: Fuse<Iter>,

    // We batch requests behind the scenes. Most requests only get the first few builds, but can
    // walk back potentially for > 50.
    cache_stack: Vec<CompletedBuild>,
    num_fetched: usize,
}

impl<'a, Iter> BuildStream<'a, Iter>
where
    Iter: Iterator<Item = BuildNumber>,
{
    fn new(builder_name: &'a str, client: &'a LLVMLabClient, ids: Iter) -> Self {
        Self {
            builder_name,
            client,
            ids: ids.fuse(),

            cache_stack: Vec::new(),
            num_fetched: 0,
        }
    }

    async fn fill_cache_stack(&mut self) -> FailureOr<Option<usize>> {
        assert!(self.cache_stack.is_empty());

        // Arbitrary limits that Should Be Good Enough.
        let num_to_fetch = if self.num_fetched == 0 {
            2
        } else {
            std::cmp::min(self.num_fetched * 2, 16)
        };

        let to_fetch: Vec<BuildNumber> = (&mut self.ids).take(num_to_fetch).collect();
        if to_fetch.is_empty() {
            return Ok(None);
        }

        self.num_fetched += to_fetch.len();
        debug!("Fetching builds {}/{:?}", self.builder_name, to_fetch);

        let statuses = self
            .client
            .fetch_build_statuses(self.builder_name, &to_fetch)
            .await?;

        for status in statuses {
            let id = status.number;
            match status.into_completed_build() {
                Ok(a) => {
                    self.cache_stack.push(a);
                }
                Err(x) => {
                    error!(
                        "Failed decoding build result for {}/{}: {}",
                        self.builder_name, id, x
                    );
                }
            }
        }

        // next() pops from the back.
        self.cache_stack.reverse();
        Ok(Some(to_fetch.len()))
    }

    async fn next(&mut self) -> FailureOr<Option<CompletedBuild>> {
        loop {
            if let Some(x) = self.cache_stack.pop() {
                return Ok(Some(x));
            }

            if self.fill_cache_stack().await?.is_none() {
                return Ok(None);
            }
        }
    }
}

// Skipping unparseable builds adds some really awkward (e.g., broken) properties here, but the
// hope is that those builds will be super rare, and we should _try_ to make progress even in the
// face of those, rather than falling over forever.

async fn update_bot_status_with_cached_builds(
    client: &LLVMLabClient,
    builder: &str,
    status: &UnabridgedBuilderStatus,
    previous: &BotStatus,
) -> FailureOr<BotStatus> {
    let new_builds: Vec<BuildNumber> = {
        let mut all_builds = status.completed_builds_ascending();
        all_builds.retain(|x| *x > previous.most_recent_build.id);
        all_builds
    };

    let mut build_stream = BuildStream::new(builder, client, new_builds.into_iter().rev());
    let newest_build = match build_stream.next().await? {
        None => return Ok(previous.clone()),
        Some(x) => x,
    };

    let is_successful_status = |s| s == BuildbotResult::Success || s == BuildbotResult::Warnings;
    if is_successful_status(newest_build.status) {
        return Ok(BotStatus {
            first_failing_build: None,
            most_recent_build: newest_build,
            state: status.state,
        });
    }

    let mut prev_build = newest_build.clone();
    let mut found_success = false;
    while let Some(build) = build_stream.next().await? {
        if is_successful_status(build.status) {
            found_success = true;
            break;
        }
        prev_build = build;
    }

    let first_failing_build = if found_success {
        prev_build
    } else if let Some(ref b) = previous.first_failing_build {
        b.clone()
    } else {
        prev_build
    };

    Ok(BotStatus {
        first_failing_build: Some(first_failing_build),
        most_recent_build: newest_build,
        state: status.state,
    })
}

async fn fetch_new_bot_status(
    client: &LLVMLabClient,
    builder: &str,
    status: &UnabridgedBuilderStatus,
) -> FailureOr<Option<BotStatus>> {
    // Fetching incrementally is helpful above, but some bots are broken pretty often upstream.
    // Grabbing their status one by one can take forever, which slows down startup quite a bit (and
    // probs places a lot of undue load on the server), so we fetch them all.

    // impl Iterator<Item = CompletedBuild>
    let new_builds = status.completed_builds_ascending();
    let mut build_stream = BuildStream::new(builder, client, new_builds.into_iter().rev());
    let newest_build = match build_stream.next().await? {
        None => return Ok(None),
        Some(x) => x,
    };

    let is_successful_status = |s| s == BuildbotResult::Success || s == BuildbotResult::Warnings;
    if is_successful_status(newest_build.status) {
        return Ok(Some(BotStatus {
            first_failing_build: None,
            most_recent_build: newest_build,
            state: status.state,
        }));
    }

    let mut prev_build = newest_build.clone();
    while let Some(build) = build_stream.next().await? {
        if is_successful_status(build.status) {
            break;
        }
        prev_build = build;
    }

    Ok(Some(BotStatus {
        first_failing_build: Some(prev_build),
        most_recent_build: newest_build,
        state: status.state,
    }))
}

// FIXME: For The Grand Plan(TM), this is kinda ugly. Ideally, this shouldn't know about BotIDs.
async fn fetch_new_status_snapshot(
    client: &LLVMLabClient,
    prev: &BotStatusSnapshot,
) -> FailureOr<HashMap<BotID, Bot>> {
    let bot_statuses =
        client
            .fetch_full_builder_status()
            .await?
            .into_iter()
            .map(|(bot_name, status)| {
                (
                    BotID {
                        master: Master::Lab,
                        name: bot_name,
                    },
                    status,
                )
            });

    let mut new_bots: HashMap<BotID, Bot> = HashMap::new();
    for (bot_id, status) in bot_statuses.into_iter() {
        // This _could_ be done in independent tasks, but we only do a lot of queries and such on
        // startup, and even that only takes ~20s to go through.
        let new_status = if let Some(last_bot) = prev.bots.get(&bot_id) {
            update_bot_status_with_cached_builds(client, &bot_id.name, &status, &last_bot.status)
                .await?
        } else {
            match fetch_new_bot_status(client, &bot_id.name, &status).await? {
                // If the bot has no builds, it's effectively dead to us.
                None => {
                    debug!("Ignoring {:?}; it has no builds", bot_id);
                    continue;
                }
                Some(x) => x,
            }
        };

        new_bots.insert(
            bot_id,
            Bot {
                category: status.category,
                status: new_status,
            },
        );
    }

    Ok(new_bots)
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
    client: LLVMLabClient,
    notifications: watch::Sender<Option<Arc<BotStatusSnapshot>>>,
) {
    let mut ticks = new_async_ticker(Duration::from_secs(60));
    let mut last_snapshot = Arc::new(BotStatusSnapshot::default());
    loop {
        ticks.recv().await.expect("ticker shut down unexpectedly");
        match fetch_new_status_snapshot(&client, &last_snapshot).await {
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
    let client = LLVMLabClient::new("http://lab.llvm.org:8011")?;
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
