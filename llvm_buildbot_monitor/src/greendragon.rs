use crate::Bot;
use crate::BotStatus;
use crate::BuildNumber;
use crate::BuildbotResult;
use crate::CompletedBuild;
use crate::Email;
use crate::FirstFailingBuild;

use std::collections::HashMap;
use std::fmt;
use std::result;

use anyhow::{Context, Result, bail};
use lazy_static::lazy_static;
use log::{info, warn};
use serde::Deserialize;

lazy_static! {
    static ref HOST: reqwest::Url =
        reqwest::Url::parse("https://ci.swift.org").expect("parsing greendragon URL");
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
    first_build: Option<RawStatusBuild>,
    jobs: Option<Vec<RawJob>>,
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
    first_build: Option<BuildNumber>,
) -> Result<FirstFailingBuild> {
    // We want the first *failure*.
    // If we have a last success, we start there and immediately jump to its 'nextBuild'.
    // If we don't have a last success, we start at the first known build.
    let (mut curr_id, mut last_success_time) = match last_successful {
        Some(s) => {
            let (raw, completed) = fetch_build_data(client, job_url, s).await?;
            let next = raw.next_build.map(|x| x.number).ok_or_else(|| {
                anyhow::anyhow!(
                    "last successful build {s} has no next build, but the bot is failing?"
                )
            })?;

            let success_time = completed
                .map(|x| x.completion_time)
                .unwrap_or(raw.timestamp.as_datetime()?);

            if next != s + 1 {
                return Ok(FirstFailingBuild::Extrapolated {
                    id: s + 1,
                    last_success_time: success_time,
                });
            }

            (next, Some(success_time))
        }
        None => (
            first_build.ok_or_else(|| anyhow::anyhow!("no builds at all for {job_url}"))?,
            None,
        ),
    };

    loop {
        let (raw, completed) = fetch_build_data(client, job_url, curr_id).await?;
        info!(
            "Checking build {curr_id} for {job_url}: result {:?}",
            raw.result
        );
        if let Some(ref x) = completed
            && x.status != BuildbotResult::Success
        {
            info!("Found first failing build for {job_url}: {curr_id}");
            return Ok(FirstFailingBuild::Known(completed.unwrap()));
        }

        let next = raw.next_build.map(|x| x.number).ok_or_else(|| {
            anyhow::anyhow!("reached end of history for {job_url} without finding a failure?")
        })?;

        if next != curr_id + 1 {
            // This is unlikely for successes, but if we just finished a success and the next build
            // is far away, we have a gap.
            return Ok(FirstFailingBuild::Extrapolated {
                id: curr_id + 1,
                last_success_time: last_success_time.unwrap_or(raw.timestamp.as_datetime()?),
            });
        }
        curr_id = next;
        last_success_time = completed.map(|x| x.completion_time);
    }
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

async fn fetch_single_bot_status_snapshot(
    client: &reqwest::Client,
    prev: Option<&Bot>,
    job: RawJob,
) -> Result<Option<Bot>> {
    let job_url = reqwest::Url::parse(&job.url)?;

    let last_build = match job.last_completed_build {
        Some(x) => x,
        None => {
            // If nothing's been done yet, just pretend the bot DNE. Not much else we can do,
            // really.
            return Ok(None);
        }
    };
    let last_build_id = last_build.number;

    let last_first_failing: Option<&FirstFailingBuild>;
    if let Some(prev_state) = prev {
        if prev_state.status.most_recent_build.id == last_build_id {
            return Ok(Some(prev_state.clone()));
        }
        last_first_failing = prev_state.status.first_failing_build.as_ref();
    } else {
        last_first_failing = None;
    }

    let first_failing_build: Option<FirstFailingBuild> = match (
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
        (None, Some(_)) => match last_first_failing {
            Some(x) => Some(x.clone()),
            None => Some(
                find_first_failing_build(client, &job_url, None, job.first_build.map(|x| x.number))
                    .await?,
            ),
        },

        (Some(s), Some(u)) => {
            if u.number > s.number {
                match last_first_failing {
                    Some(x) => Some(x.clone()),
                    None => Some(
                        find_first_failing_build(client, &job_url, Some(s.number), None).await?,
                    ),
                }
            } else {
                None
            }
        }
    };

    let most_recent_build = process_build_result(last_build.number, last_build.data)?
        .context("last completed build should have a result")?;

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
    let base_tree = "name,url,color,_class,lastCompletedBuild[number,timestamp,result,changeSet[items[authorEmail]],changeSets[items[authorEmail]]],lastSuccessfulBuild[number],lastUnsuccessfulBuild[number],firstBuild[number]";
    let tree = format!("jobs[{base_tree},jobs[{base_tree}]]");
    let overview_url = HOST.join(&format!("/job/llvm.org/view/All/api/json?tree={tree}"))?;
    let overview: JobContainer = json_get(client, overview_url).await?;
    for bot in overview.jobs {
        to_process.push((bot.name.clone(), bot));
    }
    info!("GreenDragon: {} bots to process...", to_process.len());

    let mut i = 0;
    while i < to_process.len() {
        let (display_name, job) = to_process[i].clone();
        i += 1;

        if job.color.is_some()
            && let Some(bot) =
                fetch_single_bot_status_snapshot(client, prev.get(&display_name), job.clone())
                    .await?
        {
            result.insert(display_name.clone(), bot);
        }

        if let Some(nested) = job.jobs {
            for sub_job in nested {
                to_process.push((format!("{display_name}/{}", sub_job.name), sub_job));
            }
        } else if let Some(class) = &job.class
            && (class == "org.jenkinsci.plugins.workflow.multibranch.WorkflowMultiBranchProject"
                || class == "com.cloudbees.hudson.plugins.folder.Folder")
        {
            let job_url = reqwest::Url::parse(&job.url)?;
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
                to_process.push((format!("{display_name}/{}", sub_job.name), sub_job));
            }
        }
    }

    info!("GreenDragon: done processing bots!");
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
    result: Option<RawBuildResult>,
    next_build: Option<RawStatusBuild>,

    // A single BuildResult can have either `changeSet` or `changeSets`. Both have different types.
    #[serde(default)]
    change_set: Option<ChangeSetListing>,
    #[serde(default)]
    change_sets: Vec<ChangeSetListing>,
}

fn process_build_result(id: BuildNumber, data: BuildResult) -> Result<Option<CompletedBuild>> {
    let result = match data.result {
        Some(x) => x,
        None => return Ok(None),
    };
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

    Ok(Some(CompletedBuild {
        id,
        status: match result {
            RawBuildResult::Aborted => BuildbotResult::Exception,
            RawBuildResult::Success => BuildbotResult::Success,
            // I'm not... entirely sure what 'unstable' means. Looks like it's just "test
            // failures" at a cursory glance?
            RawBuildResult::Failure | RawBuildResult::Unstable => BuildbotResult::Failure,
        },
        completion_time: data.timestamp.as_datetime()?,
        blamelist: blamelist.into(),
    }))
}

async fn fetch_build_data(
    client: &reqwest::Client,
    job_url: &reqwest::Url,
    id: BuildNumber,
) -> Result<(BuildResult, Option<CompletedBuild>)> {
    let mut api_url = job_url.clone();
    api_url
        .path_segments_mut()
        .map_err(|_| anyhow::anyhow!("invalid job URL"))?
        .pop_if_empty()
        .push(&id.to_string())
        .push("api")
        .push("json");
    let data: BuildResult = json_get(client, api_url).await?;
    let completed = process_build_result(id, data.clone())?;
    Ok((data, completed))
}
