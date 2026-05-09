use crate::Bot;
use crate::BotStatus;
use crate::BuildNumber;
use crate::BuildbotResult;
use crate::CompletedBuild;
use crate::Email;

use std::collections::HashMap;
use std::fmt;
use std::result;

use anyhow::{Context, Result, bail};
use lazy_static::lazy_static;
use log::{error, info, warn};
use serde::Deserialize;

lazy_static! {
    static ref HOST: reqwest::Url =
        reqwest::Url::parse("https://green.lab.llvm.org").expect("parsing greendragon URL");
}

async fn json_get<T>(client: &reqwest::Client, url: reqwest::Url) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let url_str = url.to_string();
    let resp = client
        .get(url)
        .send()
        .await
        .and_then(|x| x.error_for_status())
        .with_context(|| format!("requesting {url_str}"))?;

    resp.json()
        .await
        .with_context(|| format!("parsing {url_str}"))
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Color {
    // ...Which is a funny way of spelling "Green"
    Blue { flashing: bool },
    Disabled,
    Red { flashing: bool },
    Yellow { flashing: bool },
}

const VALID_COLOR_VALUES: &[(&str, Color)] = &[
    // All of the aborted builds I can find are colored grey on the UI, so.
    ("aborted", Color::Disabled),
    ("aborted_anime", Color::Disabled),
    ("blue", Color::Blue { flashing: false }),
    ("blue_anime", Color::Blue { flashing: true }),
    ("disabled", Color::Disabled),
    ("notbuilt", Color::Disabled),
    ("red", Color::Red { flashing: false }),
    ("red_anime", Color::Red { flashing: true }),
    ("yellow", Color::Yellow { flashing: false }),
    ("yellow_anime", Color::Yellow { flashing: true }),
];

struct ColorVisitor;

impl serde::de::Visitor<'_> for ColorVisitor {
    type Value = Color;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "one of {:?}",
            VALID_COLOR_VALUES
                .iter()
                .map(|x| x.0)
                .collect::<Vec<&'static str>>()
        )
    }

    fn visit_str<E>(self, s: &str) -> result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        for (name, res) in VALID_COLOR_VALUES {
            if *name == s {
                return Ok(*res);
            }
        }

        Err(E::custom(format!("{s:?} isn't a valid color")))
    }
}

impl<'de> Deserialize<'de> for Color {
    fn deserialize<D>(deserializer: D) -> result::Result<Color, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_str(ColorVisitor)
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct RawJob {
    name: String,
    url: String,
    color: Option<Color>,
    #[serde(rename = "_class")]
    class: Option<String>,
    last_completed_build: Option<BuildResultWithNumber>,
    last_successful_build: Option<RawStatusBuild>,
    last_unsuccessful_build: Option<RawStatusBuild>,
}

#[derive(Deserialize, Debug, Clone)]
struct BuildResultWithNumber {
    number: BuildNumber,
    #[serde(flatten)]
    data: BuildResult,
}

#[derive(Deserialize, Debug)]
struct JobContainer {
    jobs: Vec<RawJob>,
}

#[derive(Deserialize, Debug, Clone)]
struct RawStatusBuild {
    number: BuildNumber,
}

async fn find_first_failing_build(
    client: &reqwest::Client,
    job_url: &reqwest::Url,
    last_successful: Option<BuildNumber>,
    last_failure: BuildNumber,
) -> Result<CompletedBuild> {
    let mut api_url = job_url.clone();
    api_url
        .path_segments_mut()
        .map_err(|_| anyhow::anyhow!("invalid job URL"))?
        .pop_if_empty()
        .push("api")
        .push("json");
    let status: RawBotStatus = json_get(client, api_url).await?;
    let mut build_list = status.builds;
    build_list.sort_unstable_by_key(|x| x.number);

    debug_assert!(build_list.is_sorted_by(|x, y| x.number < y.number));

    let search_start: usize = if let Some(s) = last_successful {
        assert!(last_failure > s, "{last_failure} should be > {s}");
        match build_list.binary_search_by_key(&s, |x| x.number) {
            Ok(n) => n + 1,
            Err(n) => n,
        }
    } else {
        0
    };

    for build_number in build_list[search_start..].iter().map(|x| x.number) {
        match fetch_completed_build(client, job_url, build_number).await {
            Err(x) => {
                let root_cause = x.root_cause();
                if let Some(x) = root_cause.downcast_ref::<reqwest::Error>()
                    && x.status() == Some(reqwest::StatusCode::NOT_FOUND)
                {
                    info!(
                        "Finding first failing build for {job_url:?} 404'ed on {build_number}; trying another..."
                    );
                    continue;
                }
                return Err(x);
            }
            Ok(x) => {
                if x.status != BuildbotResult::Success {
                    return Ok(x);
                }
                error!(
                    concat!(
                        "Lies? Build {:?}/{} is reported successful, when it should've ",
                        "failed. Most recent successful == {:?}.",
                    ),
                    job_url, build_number, last_successful
                );
            }
        }
    }

    let candidates: Vec<BuildNumber> = build_list.iter().map(|x| x.number).collect();
    // This is possible if either build_list is empty, or if we raced and somehow jenkins dropped N
    // builds on the floor. So mostly just that first part.
    bail!(
        "no available builds > {:?} for {:?} (candidates: {:?})",
        last_successful,
        job_url,
        candidates
    );
}

// It's sorta interesting that the JSON has a few fields here. We have all of:
// - lastCompletedBuild
// - lastFailedBuild
// - lastStableBuild
// - lastSuccessfulBuild
// - lastUnstableBuild
// - lastUnsuccessfulBuild
// ... And it's important to note that some of these are nullable. 'Unstable' was null on the
// thing I saw, despite builds failing before, so I'm leaving that alone. So that just leaves
// unsuccessful vs failed. Since the only remaining "good" tag is lastSuccessfulBuild, let's go
// with successful/unsuccessful.
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct RawBotStatus {
    builds: Vec<RawStatusBuild>,
}

async fn fetch_single_bot_status_snapshot(
    client: &reqwest::Client,
    prev: Option<&Bot>,
    job: RawJob,
) -> Result<Option<Bot>> {
    let mut job_url = reqwest::Url::parse(&job.url)?;
    if job_url.host_str() == Some("ci.swift.org") {
        job_url.set_host(Some("green.lab.llvm.org"))?;
    }

    let last_build = match job.last_completed_build {
        Some(x) => x,
        None => {
            // If nothing's been done yet, just pretend the bot DNE. Not much else we can do,
            // really.
            return Ok(None);
        }
    };
    let last_build_id = last_build.number;

    let last_first_failing: Option<&CompletedBuild>;
    if let Some(prev_state) = prev {
        if prev_state.status.most_recent_build.id == last_build_id {
            return Ok(Some(prev_state.clone()));
        }
        last_first_failing = prev_state.status.first_failing_build.as_ref();
    } else {
        last_first_failing = None;
    }

    let first_failing_build: Option<CompletedBuild> = match (
        job.last_successful_build,
        job.last_unsuccessful_build,
    ) {
        (None, None) => {
            warn!(
                "Bot {job_url} had last build ID {last_build_id}, but no successful/unsuccessful builds"
            );
            return Ok(None);
        }
        (Some(_), None) => None,
        (None, Some(u)) => Some(match last_first_failing {
            Some(x) => x.clone(),
            None => find_first_failing_build(client, &job_url, None, u.number).await?,
        }),

        (Some(s), Some(u)) => {
            if u.number > s.number {
                match last_first_failing {
                    Some(x) => Some(x.clone()),
                    None => Some(
                        find_first_failing_build(client, &job_url, Some(s.number), u.number)
                            .await?,
                    ),
                }
            } else {
                None
            }
        }
    };

    let most_recent_build = process_build_result(last_build.number, last_build.data)?;

    Ok(Some(Bot {
        // FIXME: GreenDragon has categories and quite a few bots. Maybe use their
        // categories, too?
        category: "GreenDragon".to_owned(),
        url: job_url.to_string(),
        status: BotStatus {
            first_failing_build,
            most_recent_build,
            is_online: match job.color {
                Some(Color::Disabled) | None => false,
                Some(Color::Red { .. } | Color::Blue { .. } | Color::Yellow { .. }) => true,
            },
        },
    }))
}

pub(crate) async fn fetch_new_status_snapshot(
    client: &reqwest::Client,
    prev: &HashMap<String, Bot>,
) -> Result<HashMap<String, Bot>> {
    let mut result = HashMap::new();

    let mut to_process = Vec::new();

    // "All build groups" is necessary, since greendragon also includes a lot of miscellaneous
    // Apple-specific jobs (e.g., checking mac mini health/etc). Surfacing that probably isn't a
    // great idea.
    let tree = "jobs[name,url,color,_class,lastCompletedBuild[number,timestamp,result,changeSet[items[authorEmail]],changeSets[items[authorEmail]]],lastSuccessfulBuild[number],lastUnsuccessfulBuild[number]]";
    let overview_url = HOST.join(&format!("/job/llvm.org/view/All/api/json?tree={tree}"))?;
    let overview: JobContainer = json_get(client, overview_url).await?;
    for bot in overview.jobs {
        to_process.push((bot.name.clone(), bot));
    }

    let mut i = 0;
    while i < to_process.len() {
        let (display_name, job) = to_process[i].clone();
        i += 1;

        if job.color.is_some() {
            if let Some(bot) =
                fetch_single_bot_status_snapshot(client, prev.get(&display_name), job).await?
            {
                result.insert(display_name, bot);
            }
        } else if let Some(class) = &job.class
            && class == "org.jenkinsci.plugins.workflow.multibranch.WorkflowMultiBranchProject"
        {
            let mut job_url = reqwest::Url::parse(&job.url)?;
            if job_url.host_str() == Some("ci.swift.org") {
                job_url.set_host(Some("green.lab.llvm.org"))?;
            }
            let mut api_url = job_url.clone();
            api_url
                .path_segments_mut()
                .map_err(|_| anyhow::anyhow!("invalid job URL"))?
                .pop_if_empty()
                .push("api")
                .push("json");

            let mut tree_url = api_url.clone();
            tree_url.set_query(Some(&format!("tree={tree}")));
            let container: JobContainer = json_get(client, tree_url).await?;
            for sub_job in container.jobs {
                // We only care about the main branch for these, usually.
                // But some might have other important branches.
                // Let's include everything that has a color.
                if sub_job.color.is_some() {
                    to_process.push((format!("{display_name}/{}", sub_job.name), sub_job));
                }
            }
        }
    }

    Ok(result)
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum RawBuildResult {
    Aborted,
    Success,
    Failure,
    Unstable,
}

impl<'de> Deserialize<'de> for RawBuildResult {
    fn deserialize<D>(deserializer: D) -> result::Result<RawBuildResult, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = RawBuildResult;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "either SUCCESS, FAILURE, or ABORTED")
            }

            fn visit_str<E>(self, s: &str) -> result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match s {
                    "ABORTED" => Ok(RawBuildResult::Aborted),
                    "SUCCESS" => Ok(RawBuildResult::Success),
                    "FAILURE" => Ok(RawBuildResult::Failure),
                    "UNSTABLE" => Ok(RawBuildResult::Unstable),
                    _ => Err(E::custom(format!("{s:?} isn't a valid RawBuildResult"))),
                }
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

// N.B. This is millis; the one in lab:: is seconds.
#[derive(Copy, Clone, Debug, Deserialize)]
#[serde(transparent)]
struct RawBuildbotTime(f64);

impl RawBuildbotTime {
    fn as_datetime(self) -> Result<chrono::DateTime<chrono::Utc>> {
        let millis = self.0 as i64;
        let secs = millis / 1000;
        let nanos = ((millis % 1000) * 1_000_000) as u32;
        match chrono::DateTime::from_timestamp(secs, nanos) {
            Some(x) => Ok(x),
            None => bail!("invalid timestamp: {}", self.0),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct ChangeSet {
    author_email: String,
}

#[derive(Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct ChangeSetListing {
    items: Vec<ChangeSet>,
}

#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
struct BuildResult {
    timestamp: RawBuildbotTime,
    result: RawBuildResult,

    // A single BuildResult can have either `changeSet` or `changeSets`. Both have different types.
    #[serde(default)]
    change_set: Option<ChangeSetListing>,
    #[serde(default)]
    change_sets: Vec<ChangeSetListing>,
}

fn process_build_result(id: BuildNumber, data: BuildResult) -> Result<CompletedBuild> {
    let mut blamelist = Vec::new();
    let all_change_sets = if let Some(x) = data.change_set {
        vec![x]
    } else {
        data.change_sets
    };
    for change_sets in all_change_sets {
        for change_set in change_sets.items {
            match Email::parse(&change_set.author_email) {
                Some(x) => blamelist.push(x),
                None => warn!("Unparseable email: {:?}", &change_set.author_email),
            }
        }
    }

    Ok(CompletedBuild {
        id,
        status: match data.result {
            RawBuildResult::Aborted => BuildbotResult::Exception,
            RawBuildResult::Success => BuildbotResult::Success,
            // I'm not... entirely sure what 'unstable' means. Looks like it's just "test
            // failures" at a cursory glance?
            RawBuildResult::Failure | RawBuildResult::Unstable => BuildbotResult::Failure,
        },
        completion_time: data.timestamp.as_datetime()?,
        blamelist: blamelist.into(),
    })
}

async fn fetch_completed_build(
    client: &reqwest::Client,
    job_url: &reqwest::Url,
    id: BuildNumber,
) -> Result<CompletedBuild> {
    let mut api_url = job_url.clone();
    api_url
        .path_segments_mut()
        .map_err(|_| anyhow::anyhow!("invalid job URL"))?
        .pop_if_empty()
        .push(&id.to_string())
        .push("api")
        .push("json");
    let data: BuildResult = json_get(client, api_url).await?;
    process_build_result(id, data)
}
