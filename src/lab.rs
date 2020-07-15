use crate::try_with_context;
use crate::Bot;
use crate::BotStatus;
use crate::BuildNumber;
use crate::BuildbotResult;
use crate::BuilderState;
use crate::CompletedBuild;
use crate::Email;
use crate::FailureOr;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::iter::Fuse;

use failure::bail;
use lazy_static::lazy_static;
use log::{debug, error, warn};
use serde::Deserialize;

struct BuilderStateVisitor;

fn valid_builder_state_values() -> &'static [(&'static str, BuilderState)] {
    &[
        ("building", BuilderState::Building),
        ("idle", BuilderState::Idle),
        ("offline", BuilderState::Offline),
    ]
}

impl<'de> serde::de::Visitor<'de> for BuilderStateVisitor {
    type Value = BuilderState;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "one of {:?}",
            valid_builder_state_values()
                .iter()
                .map(|x| x.0)
                .collect::<Vec<&'static str>>()
        )
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        for (name, res) in valid_builder_state_values() {
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
    // This may be either omitted (unsure why) or null (buildbot was killed in the middle of a
    // build).
    #[serde(default)]
    results: Option<RawBuildbotResult>,
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
        let status = match self.results {
            Some(x) => x.as_buildbot_result()?,
            None => bail!("no `results` field available"),
        };

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
            status,
            completion_time,
            blamelist,
        })
    }
}

lazy_static! {
    static ref HOST: reqwest::Url =
        reqwest::Url::parse("http://lab.llvm.org:8011").expect("parsing lab URL");
}

async fn json_get<T>(client: &reqwest::Client, path: &str) -> FailureOr<T>
where
    T: serde::de::DeserializeOwned,
{
    let resp = try_with_context!(
        client
            .get(HOST.join(path)?)
            .send()
            .await
            .and_then(|x| x.error_for_status()),
        "requesting {}",
        path
    );

    Ok(try_with_context!(resp.json().await, "parsing {}", path))
}

async fn fetch_full_builder_status(
    client: &reqwest::Client,
) -> FailureOr<HashMap<String, UnabridgedBuilderStatus>> {
    Ok(json_get(client, "/json/builders").await?)
}

async fn fetch_build_statuses(
    client: &reqwest::Client,
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

    let mut results: HashMap<String, UnabridgedBuildStatus> = json_get(client, &url).await?;
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

struct BuildStream<'a, Iter>
where
    Iter: Iterator<Item = BuildNumber>,
{
    builder_name: &'a str,
    client: &'a reqwest::Client,
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
    fn new(builder_name: &'a str, client: &'a reqwest::Client, ids: Iter) -> Self {
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

        for status in fetch_build_statuses(self.client, self.builder_name, &to_fetch).await? {
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
    client: &reqwest::Client,
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
    client: &reqwest::Client,
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

pub(crate) async fn fetch_new_status_snapshot(
    client: &reqwest::Client,
    prev: &HashMap<String, Bot>,
) -> FailureOr<HashMap<String, Bot>> {
    let bot_statuses = fetch_full_builder_status(client).await?;

    let mut new_bots: HashMap<String, Bot> = HashMap::new();
    for (bot_name, status) in bot_statuses.into_iter() {
        // This _could_ be done in independent tasks, but we only do a lot of queries and such on
        // startup, and even that only takes ~20s to go through.
        let new_status = if let Some(last_bot) = prev.get(&bot_name) {
            update_bot_status_with_cached_builds(client, &bot_name, &status, &last_bot.status)
                .await?
        } else {
            match fetch_new_bot_status(client, &bot_name, &status).await? {
                // If the bot has no builds, it's effectively dead to us.
                None => {
                    debug!("Ignoring {:?}; it has no builds", bot_name);
                    continue;
                }
                Some(x) => x,
            }
        };

        new_bots.insert(
            bot_name,
            Bot {
                category: status.category,
                status: new_status,
            },
        );
    }

    Ok(new_bots)
}
