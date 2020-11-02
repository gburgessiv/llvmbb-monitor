use crate::Bot;
use crate::BotStatus;
use crate::BuildNumber;
use crate::BuildbotResult;
use crate::CompletedBuild;
use crate::Email;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, Context, Result};
use lazy_static::lazy_static;
use log::{debug, info, warn};
use serde::Deserialize;

pub(crate) type BotID = u32;

pub(crate) const MAX_CONCURRENCY: usize = 4;

// Buildbot has two flavors of build IDs: build IDs _local to a bot_, and _globally unique_
// build IDs. Represent those as distinct types, so they're difficult to inadvertently flip.
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Deserialize)]
#[serde(transparent)]
struct GlobalBuildNumber(BuildNumber);

impl GlobalBuildNumber {
    fn as_global_crate_build_number(&self) -> BuildNumber {
        self.0
    }
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Deserialize)]
#[serde(transparent)]
struct LocalBuildNumber(BuildNumber);

impl LocalBuildNumber {
    fn as_local_crate_build_number(&self) -> BuildNumber {
        self.0
    }
}

lazy_static! {
    static ref HOST: reqwest::Url =
        reqwest::Url::parse("http://lab.llvm.org:8011/api/v2/").expect("parsing lab URL");
}

fn find_chained_error_of_type<Src: std::error::Error + 'static, E: std::error::Error + 'static>(
    base: &E,
) -> Option<&Src> {
    let mut current: &dyn std::error::Error = base;
    while let Some(e) = current.source() {
        if let Some(x) = e.downcast_ref::<Src>() {
            return Some(x);
        }
        current = e;
    }
    None
}

fn is_incomplete_message_error(e: &reqwest::Error) -> bool {
    if let Some(e) = find_chained_error_of_type::<hyper::Error, _>(e) {
        if e.is_incomplete_message() {
            return true;
        }

        if let Some(e) = find_chained_error_of_type::<std::io::Error, _>(e) {
            return e.kind() == std::io::ErrorKind::ConnectionReset;
        }
    }
    false
}

async fn json_get_api<T>(client: &reqwest::Client, url: &reqwest::Url) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let max_attempts = 5usize;
    let mut attempt_number = 0usize;
    let url_str = format!("{}", url);
    loop {
        let resp = match client
            .get(url.clone())
            .send()
            .await
            .and_then(|x| x.error_for_status())
        {
            Err(x) => {
                // Occasionally, the lab will shut down idle connections. There's a race here
                // between "the connection is closing" (which requires network packets to be
                // shuffled around in order to complete) and "the application wants to send bits on
                // the wire of this connection."
                //
                // Ultimately, there's no way to fix this, but retrying a few times should paper
                // over it.
                if attempt_number < max_attempts && is_incomplete_message_error(&x) {
                    warn!(
                        "Request to {:?} failed due to apparent connection shutdown; retrying: {}",
                        url_str, x,
                    );
                    attempt_number += 1;
                    continue;
                }

                return Err(anyhow::Error::new(x).context(format!("requesting {:?}", url_str)));
            }
            Ok(x) => x,
        };

        return Ok(resp
            .json()
            .await
            .with_context(|| format!("parsing {:?}", url_str))?);
    }
}

fn is_successful_status(s: BuildbotResult) -> bool {
    s == BuildbotResult::Success || s == BuildbotResult::Warnings
}

#[derive(Debug, Deserialize)]
struct BuilderInfo {
    #[serde(rename = "builderid")]
    id: BotID,
    name: String,
    tags: Vec<String>,
}

#[derive(Deserialize)]
struct BuildersResult {
    builders: Vec<BuilderInfo>,
}

async fn fetch_builder_info(client: &reqwest::Client, id: BotID) -> Result<BuilderInfo> {
    let mut builders =
        json_get_api::<BuildersResult>(client, &HOST.join(&format!("builders/{}", id))?)
            .await?
            .builders;

    if builders.len() != 1 {
        bail!(
            "bot ID {} matches {} bots; should match exactly 1",
            id,
            builders.len()
        );
    }
    Ok(builders.pop().unwrap())
}

async fn fetch_builder_infos(client: &reqwest::Client) -> Result<Vec<BuilderInfo>> {
    Ok(
        json_get_api::<BuildersResult>(client, &HOST.join("builders")?)
            .await?
            .builders,
    )
}

// http://docs.buildbot.net/current/developer/results.html#build-result-codes
#[derive(Copy, Clone, Debug)]
struct RawBuildbotResult(BuildbotResult);

impl<'de> serde::de::Deserialize<'de> for RawBuildbotResult {
    fn deserialize<D>(deserializer: D) -> Result<RawBuildbotResult, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de2> serde::de::Visitor<'de2> for Visitor {
            type Value = RawBuildbotResult;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an integer between [0,6]")
            }

            fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryInto;
                match value.try_into() {
                    Ok(x) => self.visit_i64(x),
                    Err(_) => Err(E::custom(format!(
                        "{} is an invalid buildbot result",
                        value
                    ))),
                }
            }

            fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    0 => Ok(RawBuildbotResult(BuildbotResult::Success)),
                    1 => Ok(RawBuildbotResult(BuildbotResult::Warnings)),
                    2 => Ok(RawBuildbotResult(BuildbotResult::Failure)),
                    3 => Ok(RawBuildbotResult(BuildbotResult::Skipped)),
                    4 => Ok(RawBuildbotResult(BuildbotResult::Exception)),
                    // 5 is technically 'RETRY'. For our purposes, that's an exception.
                    5 => Ok(RawBuildbotResult(BuildbotResult::Exception)),
                    // 6 is technically 'CANCELLED'. For our purposes, that's an exception.
                    6 => Ok(RawBuildbotResult(BuildbotResult::Exception)),
                    n => Err(E::custom(format!("{} is an invalid buildbot result", n))),
                }
            }
        }
        deserializer.deserialize_i64(Visitor)
    }
}

#[derive(Copy, Clone, Debug)]
struct RawBuildbotTime(chrono::NaiveDateTime);

impl<'de> serde::de::Deserialize<'de> for RawBuildbotTime {
    fn deserialize<D>(deserializer: D) -> Result<RawBuildbotTime, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de2> serde::de::Visitor<'de2> for Visitor {
            type Value = RawBuildbotTime;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a floating-point number representing a time")
            }

            fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let secs = value as i64;
                let nanos = ((value - secs as f64) * 1_000_000_000f64) as u32;
                match chrono::NaiveDateTime::from_timestamp_opt(secs, nanos) {
                    Some(x) => Ok(RawBuildbotTime(x)),
                    None => Err(E::custom(format!("{} is an invalid timestamp", value))),
                }
            }

            fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryInto;
                match value.try_into() {
                    Ok(x) => self.visit_i64(x),
                    Err(_) => Err(E::custom(format!("{} is an invalid timestamp", value))),
                }
            }

            fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let secs = value as i64;
                match chrono::NaiveDateTime::from_timestamp_opt(secs, 0) {
                    Some(x) => Ok(RawBuildbotTime(x)),
                    None => Err(E::custom(format!("{} is an invalid timestamp", value))),
                }
            }
        }
        deserializer.deserialize_f64(Visitor)
    }
}

#[derive(Debug, Clone)]
struct CompletedLabBuild {
    builder_id: BotID,
    build_id: GlobalBuildNumber,
    local_build_id: LocalBuildNumber,
    complete_at: chrono::NaiveDateTime,
    result: BuildbotResult,
}

#[derive(Debug, Clone)]
struct BuilderBuildInfo {
    most_recent_build: CompletedLabBuild,
    first_failing_build: Option<CompletedLabBuild>,
    // A list of pending builds that were _encountered_ in finding the first failing build. If lots
    // of builds are in progress, this might not be a complete listing.
    pending_builds: Vec<GlobalBuildNumber>,
}

#[derive(Clone, Debug, Deserialize)]
struct UnabridgedLabBuild {
    #[serde(rename = "builderid")]
    builder_id: BotID,
    #[serde(rename = "buildid")]
    build_id: GlobalBuildNumber,
    #[serde(rename = "number")]
    local_build_id: LocalBuildNumber,
    // If not specified, the build isn't done.
    #[serde(rename = "results", default)]
    result: Option<RawBuildbotResult>,
    // If not specified, the build isn't done.
    #[serde(default)]
    complete_at: Option<RawBuildbotTime>,
}

impl UnabridgedLabBuild {
    fn to_completed_build(&self) -> Option<CompletedLabBuild> {
        if let (Some(complete_at), Some(result)) = (self.complete_at, self.result) {
            Some(CompletedLabBuild {
                builder_id: self.builder_id,
                build_id: self.build_id,
                local_build_id: self.local_build_id,
                complete_at: complete_at.0,
                result: result.0,
            })
        } else {
            None
        }
    }
}

#[derive(Deserialize)]
struct UnabridgedLabBuildListing {
    builds: Vec<UnabridgedLabBuild>,
}

async fn fetch_builder_build_info<T: std::borrow::Borrow<reqwest::Client>>(
    client: T,
    builder_id: BotID,
) -> Result<Option<BuilderBuildInfo>> {
    debug!("Fetching builder info for {}", builder_id);

    let fetch_amount = 25;
    // If we have to go more than this many builds back, really, we're done.
    let max_fetch = 500usize;
    let mut best_result: Option<(CompletedLabBuild, CompletedLabBuild)> = None;

    let fetch_amount_str = fetch_amount.to_string();
    let mut pending_builds = Vec::new();
    let mut query_url = HOST.join(&format!("builders/{}/builds", builder_id))?;
    for start in (0..max_fetch).step_by(fetch_amount) {
        query_url
            .query_pairs_mut()
            .clear()
            .append_pair("order", "-buildid")
            .append_pair("limit", &fetch_amount_str)
            .append_pair("offset", &start.to_string());

        let results = json_get_api::<UnabridgedLabBuildListing>(client.borrow(), &query_url)
            .await?
            .builds;
        if results.is_empty() {
            break;
        }

        // Races: UnabridgedLabBuilds are assumed to be immutable once their result/complete_at
        // fields are set. We only examine UnabridgedLabBuilds that have those set. If, by
        // happenstance, one or more builds get added during our search, that's OK: we'll just
        // rescan old ones and re-discard them as we've done previously.
        let host_returned_fewer_builds_than_requested = results.len() < fetch_amount;
        let completed_results: Vec<_> = results
            .into_iter()
            .filter_map(|x| {
                if let Some(c) = x.to_completed_build() {
                    Some(c)
                } else {
                    pending_builds.push(x.build_id);
                    None
                }
            })
            .collect();

        let newest_success = completed_results
            .iter()
            .enumerate()
            .find(|(_, x)| is_successful_status(x.result));
        match newest_success {
            Some((i, _)) => match best_result {
                None => {
                    if i == 0 {
                        return Ok(Some(BuilderBuildInfo {
                            most_recent_build: completed_results[0].clone(),
                            first_failing_build: None,
                            pending_builds,
                        }));
                    }
                    return Ok(Some(BuilderBuildInfo {
                        most_recent_build: completed_results[0].clone(),
                        first_failing_build: Some(completed_results[i - 1].clone()),
                        pending_builds,
                    }));
                }
                Some((first_failing_build, most_recent_build)) => {
                    return Ok(Some(BuilderBuildInfo {
                        most_recent_build,
                        first_failing_build: Some(
                            completed_results
                                .get(i - 1)
                                .unwrap_or(&first_failing_build)
                                .clone(),
                        ),
                        pending_builds,
                    }));
                }
            },
            None if completed_results.is_empty() => (),
            None => {
                let most_recent_build = completed_results.first().unwrap().clone();
                let first_failing_build = completed_results.last().unwrap().clone();
                best_result = Some((first_failing_build, most_recent_build));
            }
        }

        // If we got fewer builds than we requested, we're out.
        if host_returned_fewer_builds_than_requested {
            break;
        }
    }

    match best_result {
        Some((first_failing_build, most_recent_build)) => Ok(Some(BuilderBuildInfo {
            most_recent_build,
            first_failing_build: Some(first_failing_build),
            pending_builds,
        })),
        None => Ok(None),
    }
}

async fn concurrent_map_early_exit<
    A: Send + Sync + 'static,
    T: Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<T>> + Send,
    Run: Fn(A) -> Fut + 'static + Send + Sync,
    IterA: std::iter::IntoIterator<Item = A>,
>(
    jobs: usize,
    items: IterA,
    run: Run,
) -> Result<Vec<T>> {
    assert_ne!(jobs, 0);

    let (num_requests, request_stack) = {
        let mut x: Vec<(usize, A)> = items.into_iter().enumerate().collect();
        // Micro-opt: we do a lot of work below for nothing if nothing's here.
        if x.is_empty() {
            return Ok(Vec::new());
        }

        x.reverse();
        (x.len(), Arc::new(Mutex::new(x)))
    };

    let (resp_send, mut resp_recv) = tokio::sync::mpsc::channel::<(usize, Result<T>)>(jobs);
    let jobs = std::cmp::min(num_requests, jobs);
    let run = Arc::new(run);
    let join_handles: Vec<_> = (0..jobs)
        .map(|_| {
            let request_stack = request_stack.clone();
            let mut resp_send = resp_send.clone();
            let run = run.clone();
            tokio::spawn(async move {
                loop {
                    let (index, arg) = match request_stack.lock().unwrap().pop() {
                        Some(x) => x,
                        None => return,
                    };

                    // The other side wants to shut down.
                    if let Err(_) = resp_send.send((index, run(arg).await)).await {
                        return;
                    }
                }
            })
        })
        .collect();
    std::mem::drop(resp_send);

    let mut results = Vec::with_capacity(num_requests);
    for _ in 0..num_requests {
        results.push(None);
    }

    while let Some((index, x)) = resp_recv.recv().await {
        match x {
            Err(x) => {
                std::mem::drop(resp_recv);
                for h in join_handles {
                    let _ = h.await;
                }
                return Err(x);
            }
            Ok(x) => results[index] = Some(x),
        }
    }

    let results = results.into_iter().map(|x| x.unwrap()).collect();
    for h in join_handles {
        let _ = h.await;
    }
    Ok(results)
}

/// "Foo Bar <foo@bar.com>" => "foo@bar.com"
/// Otherwise, returns the original string unaltered.
fn remove_name_from_email(email: &str) -> &str {
    match (email.rfind('<'), email.rfind('>')) {
        (Some(x), Some(y)) if x < y => &email[x + 1..y],
        _ => email,
    }
}

async fn fetch_build_blamelist(
    client: &reqwest::Client,
    build_id: GlobalBuildNumber,
) -> Result<Vec<Email>> {
    #[derive(Debug, Deserialize)]
    struct UnabridgedChange {
        author: String,
    }

    #[derive(Debug, Deserialize)]
    struct UnabridgedChangesListing {
        changes: Vec<UnabridgedChange>,
    }

    let query_url = HOST.join(&format!(
        "builds/{}/changes",
        build_id.as_global_crate_build_number()
    ))?;
    let results = json_get_api::<UnabridgedChangesListing>(client, &query_url)
        .await?
        .changes;

    results
        .into_iter()
        .map(|x| {
            Email::parse(remove_name_from_email(&x.author))
                .ok_or_else(|| anyhow!("failed parsing email: {:?}", x))
        })
        .collect()
}

async fn resolve_completed_lab_build<T: std::borrow::Borrow<reqwest::Client>>(
    client: T,
    build: &CompletedLabBuild,
) -> Result<CompletedBuild> {
    let blamelist = fetch_build_blamelist(client.borrow(), build.build_id).await?;
    Ok(CompletedBuild {
        id: build.local_build_id.as_local_crate_build_number(),
        status: build.result,
        completion_time: build.complete_at,
        blamelist,
    })
}

fn determine_bot_category(bot_info: &BuilderInfo) -> Option<&str> {
    // Bot categories are... interesting. Older verisons of the lab had exactly one category per
    // bot, but newer versions only have the notion of tags, aaaand a single bot may have multiple
    // tags.

    // If there's precisely one tag though, we have a clear winner.
    if bot_info.tags.len() == 1 {
        return Some(&bot_info.tags[0]);
    }

    // Some bots have a `toolchain` tag on them, and their names are otherwise not possible to
    // extract a category from. Prioritize that tag.
    {
        let toolchain_tag = "toolchain";
        if bot_info.tags.iter().any(|x| x == toolchain_tag) {
            return Some(toolchain_tag);
        }
    }

    // Similarly, `clang-tools` bots break word splitting below, since we split on /[-_]/. If we
    // find a clang-tools tag, that wins.
    {
        let clang_tools = "clang-tools";
        if bot_info.tags.iter().any(|x| x == clang_tools) {
            return Some(clang_tools);
        }
    }

    // ...But if there's more than one tag, go hunting in the name. An approach that seemed
    // reasonable at first was "hey, just take the first word of the bot's name." This works for
    // most bots, but not others:
    // - llvm-clang-foo-bar should be in "clang"
    // - ppc64le-lld-foo-bar should be in "lld"
    // - aosp-O3-polly-before-vectorizer-unprofitable should be in "polly"
    // etc etc.
    //
    // So we split the name, looking for known categories in the split. If we find more than zero,
    // we prioritize these categories based on a predetermined 'importance list', and return the
    // winner. Directly below is said importance list. Ordering was intuit'ed by glancing at
    // http://lab.llvm.org:8011/#/builders.
    let known_categories = [
        "clang",
        "polly",
        "flang",
        "lld",
        "libc",
        "libcxx",
        "libunwind",
        "lldb",
        "llvm",
        "rev_iter",
    ];

    let best_index = bot_info
        .name
        .split(|c| c == '_' || c == '-')
        .filter_map(|piece| {
            known_categories
                .iter()
                .enumerate()
                .find(|(_, x)| **x == piece)
                .map(|(index, _)| index)
        })
        .min();
    best_index.map(|x| known_categories[x])
}

async fn resolve_builder_build_info(
    client: &reqwest::Client,
    bot_info: &BuilderInfo,
    info: &BuilderBuildInfo,
) -> Result<Bot> {
    let most_recent_build = resolve_completed_lab_build(client, &info.most_recent_build).await?;
    let first_failing_build = match &info.first_failing_build {
        None => None,
        Some(x) => Some(resolve_completed_lab_build(client, &x).await?),
    };
    Ok(Bot {
        category: determine_bot_category(bot_info)
            .unwrap_or(&bot_info.name)
            .to_string(),
        status: BotStatus {
            first_failing_build,
            most_recent_build,
            is_online: true, // FIXME: having an actual value for this would be nice...
        },
    })
}

async fn perform_initial_builder_sync(
    client: &reqwest::Client,
) -> Result<(LabState, HashMap<BotID, (String, Bot)>)> {
    info!("Beginning full sync for the lab");
    let builder_infos = fetch_builder_infos(client).await?;
    info!("There appear to be {} builders", builder_infos.len());

    let actual_builds: Vec<Option<BuilderBuildInfo>> = {
        let client = client.clone();
        concurrent_map_early_exit(
            MAX_CONCURRENCY,
            builder_infos.iter().map(|x| x.id),
            move |builder_id: BotID| {
                let client = client.clone();
                async move {
                    fetch_builder_build_info(client, builder_id)
                        .await
                        .with_context(|| format!("fetching builder {} status", builder_id))
                }
            },
        )
        .await?
    };

    let mut pending_builds = Vec::new();
    for x in &actual_builds {
        if let Some(x) = x {
            pending_builds.extend(&x.pending_builds);
        }
    }

    let most_recent_build = actual_builds
        .iter()
        .filter_map(|build| build.as_ref().map(|x| x.most_recent_build.build_id))
        .max()
        .ok_or_else(|| anyhow!("no builds found"))?;

    let resolved_builds: Vec<(BotID, (String, Bot))> = {
        let client = client.clone();
        concurrent_map_early_exit(
            MAX_CONCURRENCY,
            builder_infos
                .into_iter()
                .zip(actual_builds.into_iter())
                .filter_map(|(bot_info, status)| match status {
                    None => {
                        info!("Dropping {:?}; it has no builds", bot_info.name);
                        None
                    }
                    Some(x) => Some((bot_info, x)),
                }),
            move |(bot_info, actual_build)| {
                let client = client.clone();
                async move {
                    let result =
                        resolve_builder_build_info(&client, &bot_info, &actual_build).await?;
                    Ok((bot_info.id, (bot_info.name, result)))
                }
            },
        )
        .await?
    };

    debug!("Loaded information for builders: {:?}", {
        let mut x = resolved_builds
            .iter()
            .map(|(_, (name, _))| name)
            .collect::<Vec<_>>();
        x.sort();
        x
    });

    info!("Handing back info for {} builders", resolved_builds.len());

    let lab_state = LabState {
        most_recent_build,
        pending_builds,
    };
    Ok((lab_state, resolved_builds.into_iter().collect()))
}

pub(crate) struct LabState {
    most_recent_build: GlobalBuildNumber,
    pending_builds: Vec<GlobalBuildNumber>,
}

// Fetches all builds, stopping _fetching_ once it encounters `stop_at`. Importantly, it may hand
// back _more results older than stop_at_.
async fn fetch_latest_build_statuses(
    client: &reqwest::Client,
    stop_at: GlobalBuildNumber,
) -> Result<Vec<UnabridgedLabBuild>> {
    #[derive(Deserialize, Debug)]
    struct BuildsResult {
        builds: Vec<UnabridgedLabBuild>,
    }

    let mut results = Vec::new();
    // We have 100ish builders; 300 should generally allow us to fetch everything we care about in
    // one try.
    let fetch_amount = 300;
    let fetch_amount_str = fetch_amount.to_string();
    let mut query_url = HOST.join("builds")?;
    for start in (0usize..).step_by(fetch_amount) {
        query_url
            .query_pairs_mut()
            .clear()
            .append_pair("order", "-buildid")
            .append_pair("limit", &fetch_amount_str)
            .append_pair("offset", &start.to_string());

        let page = json_get_api::<BuildsResult>(client, &query_url)
            .await?
            .builds;

        let stop = page.len() < fetch_amount || page.iter().any(|x| x.build_id <= stop_at);
        results.extend(page.into_iter());
        if stop {
            return Ok(results);
        }
    }

    unreachable!();
}

async fn fetch_build_by_id(
    client: reqwest::Client,
    number: GlobalBuildNumber,
) -> Result<UnabridgedLabBuild> {
    #[derive(Deserialize)]
    struct Builds {
        builds: Vec<UnabridgedLabBuild>,
    }

    let mut builds = json_get_api::<Builds>(
        &client,
        &HOST.join(&format!("builds/{}", number.as_global_crate_build_number()))?,
    )
    .await?
    .builds;
    if builds.len() != 1 {
        bail!(
            "build ID {:?} matches {} builds; should match exactly 1",
            number,
            builds.len()
        );
    }
    Ok(builds.pop().unwrap())
}

async fn perform_incremental_builder_sync(
    client: &reqwest::Client,
    prev_state: &LabState,
    prev_result: &HashMap<BotID, (String, Bot)>,
) -> Result<(LabState, HashMap<BotID, (String, Bot)>)> {
    let build_status_snapshot =
        fetch_latest_build_statuses(client, prev_state.most_recent_build).await?;

    let most_recent_build = build_status_snapshot
        .iter()
        .map(|x| x.build_id)
        .max()
        .ok_or_else(|| anyhow!("no builds returned by builds/?"))?;

    let missed_builds: Vec<UnabridgedLabBuild> = {
        let fetched_ids: HashSet<GlobalBuildNumber> =
            build_status_snapshot.iter().map(|x| x.build_id).collect();
        let client = client.clone();
        concurrent_map_early_exit(
            MAX_CONCURRENCY,
            prev_state
                .pending_builds
                .iter()
                .cloned()
                .filter(|x| !fetched_ids.contains(&x)),
            move |id| fetch_build_by_id(client.clone(), id),
        )
        .await?
    };

    let previously_pending_builds: HashSet<GlobalBuildNumber> =
        prev_state.pending_builds.iter().cloned().collect();
    let mut pending_builds = Vec::new();

    let completed_builds: Vec<CompletedLabBuild> = build_status_snapshot
        .into_iter()
        .chain(missed_builds.into_iter())
        .filter_map(|build| {
            if let Some(c) = build.to_completed_build() {
                Some(c)
            } else {
                pending_builds.push(build.build_id);
                None
            }
        })
        .collect();

    let resolved_newly_completed_builds: Vec<(BotID, CompletedBuild)> = {
        let client = client.clone();
        concurrent_map_early_exit(
            MAX_CONCURRENCY,
            completed_builds.into_iter().filter(|x| {
                x.build_id > prev_state.most_recent_build
                    || previously_pending_builds.contains(&x.build_id)
            }),
            move |build| {
                let client = client.clone();
                async move {
                    let result = resolve_completed_lab_build(client, &build).await?;
                    Ok((build.builder_id, result))
                }
            },
        )
        .await?
    };

    let mut new_results = HashMap::new();
    for (bot_id, build) in resolved_newly_completed_builds {
        let (name, new_state): (String, Bot) = match prev_result.get(&bot_id) {
            Some((name, bot)) => {
                // FIXME: It is. Theoretically possible for an old build for a bot to finish
                // before a new one. If there are multiple actual bots servicing the same
                // "bot". In that case... ugh. It doesn't _seem_ that we make use of this
                // flexibility at the moment, but our best bet is probably to delay
                // surfacing that a build finished until all builds before it are done.
                // Otherwise, we may blame the wrong set of people, and have all sorts of all
                // fun other effects downstream if a first_failing_build travels back in time.
                //
                // FIXME: is_online freshness?
                (
                    name.clone(),
                    Bot {
                        category: bot.category.clone(),
                        status: BotStatus {
                            first_failing_build: if is_successful_status(build.status) {
                                None
                            } else if let Some(x) = bot.status.first_failing_build.as_ref() {
                                Some(x.clone())
                            } else {
                                Some(build.clone())
                            },
                            most_recent_build: build,
                            is_online: true,
                        },
                    },
                )
            }
            None => {
                warn!(
                    "Previous state had no builds for {:?}; full-sync'ing it",
                    bot_id
                );
                let bot_info = fetch_builder_info(client, bot_id).await?;
                warn!("Synced {:?}'s name == {:?}", bot_id, bot_info.name,);
                let bot = match fetch_builder_build_info(client, bot_id).await? {
                    Some(build_info) => {
                        resolve_builder_build_info(client, &bot_info, &build_info).await?
                    }
                    None => {
                        warn!(
                            "...Somehow, {:?} had no builds? Full syncing everything.",
                            bot_id
                        );
                        return perform_initial_builder_sync(client).await;
                    }
                };
                (bot_info.name, bot)
            }
        };
        new_results.insert(bot_id, (name, new_state));
    }

    for (id, status) in prev_result {
        if !new_results.contains_key(id) {
            new_results.insert(*id, status.clone());
        }
    }

    let new_lab_state = LabState {
        most_recent_build,
        pending_builds,
    };
    Ok((new_lab_state, new_results))
}

pub(crate) async fn fetch_new_status_snapshot(
    client: &reqwest::Client,
    lab_state: &mut Option<LabState>,
    prev_result: &HashMap<BotID, (String, Bot)>,
) -> Result<HashMap<BotID, (String, Bot)>> {
    let (new_state, results) = match lab_state {
        None => perform_initial_builder_sync(client).await?,
        Some(state) => perform_incremental_builder_sync(client, state, prev_result).await?,
    };
    *lab_state = Some(new_state);
    Ok(results)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_category_determinations() {
        assert_eq!(
            Some("llvm"),
            determine_bot_category(&BuilderInfo {
                id: 123,
                name: "llvm-sphinx-docs".to_string(),
                tags: vec!["llvm".into(), "docs".into()],
            })
        );
    }
}
