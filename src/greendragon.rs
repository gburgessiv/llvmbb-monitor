use crate::try_with_context;
use crate::Bot;
use crate::BotStatus;
use crate::BuildNumber;
use crate::BuildbotResult;
use crate::BuilderState;
use crate::CompletedBuild;
use crate::Email;
use crate::FailureOr;

use std::collections::HashMap;
use std::fmt;

use failure::bail;
use lazy_static::lazy_static;
use log::{error, info, warn};
use serde::Deserialize;

#[derive(Deserialize)]
struct UnabridgedBuildStatus;

#[derive(Deserialize)]
struct UnabridgedBuilderStatus;

lazy_static! {
    static ref HOST: reqwest::Url =
        reqwest::Url::parse("http://green.lab.llvm.org").expect("parsing greendragon URL");
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

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Color {
    // ...Which is a funny way of spelilng "Green"
    Blue { flashing: bool },
    Disabled,
    Red { flashing: bool },
}

fn valid_color_values() -> &'static [(&'static str, Color)] {
    &[
        // All of the aborted builds I can find are colored grey on the UI, so.
        ("aborted", Color::Disabled),
        ("aborted_anime", Color::Disabled),
        ("blue", Color::Blue { flashing: false }),
        ("blue_anime", Color::Blue { flashing: true }),
        ("disabled", Color::Disabled),
        ("notbuilt", Color::Disabled),
        ("red", Color::Red { flashing: false }),
        ("red_anime", Color::Red { flashing: true }),
    ]
}

struct ColorVisitor;

impl<'de> serde::de::Visitor<'de> for ColorVisitor {
    type Value = Color;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "one of {:?}",
            valid_color_values()
                .iter()
                .map(|x| x.0)
                .collect::<Vec<&'static str>>()
        )
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        for (name, res) in valid_color_values() {
            if *name == s {
                return Ok(*res);
            }
        }

        Err(E::custom(format!("{:?} isn't a valid color", s)))
    }
}

impl<'de> Deserialize<'de> for Color {
    fn deserialize<D>(deserializer: D) -> Result<Color, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_str(ColorVisitor)
    }
}

#[derive(Deserialize, Debug)]
struct SingleBotOverview {
    name: String,
    color: Color,
}

#[derive(Deserialize, Debug)]
struct AllBotsOverview {
    jobs: Vec<SingleBotOverview>,
}

#[derive(Deserialize, Debug)]
struct RawStatusBuild {
    number: BuildNumber,
}

async fn find_first_failing_build(
    client: &reqwest::Client,
    bot_name: &str,
    build_list: &[RawStatusBuild],
    last_successful: Option<BuildNumber>,
    last_failure: BuildNumber,
) -> FailureOr<CompletedBuild> {
    debug_assert!(build_list.is_sorted_by_key(|x| x.number));

    let search_start: usize = if let Some(s) = last_successful {
        assert!(last_failure > s, "{} should be > {}", last_failure, s);
        match build_list.binary_search_by_key(&s, |x| x.number) {
            Ok(n) => n + 1,
            Err(n) => n,
        }
    } else {
        0
    };

    for build_number in build_list[search_start..].iter().map(|x| x.number) {
        match fetch_completed_build(client, bot_name, build_number).await {
            Err(x) => {
                let root_cause = x.find_root_cause();
                if let Some(x) = root_cause.downcast_ref::<reqwest::Error>() {
                    if x.status() == Some(reqwest::StatusCode::NOT_FOUND) {
                        info!(
                            "Finding first failing build for {:?} 404'ed on {}; trying another...",
                            bot_name, build_number
                        );
                        continue;
                    }
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
                    bot_name, build_number, last_successful
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
        bot_name,
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
    last_completed_build: Option<RawStatusBuild>,
    last_successful_build: Option<RawStatusBuild>,
    last_unsuccessful_build: Option<RawStatusBuild>,
}

async fn fetch_single_bot_status_snapshot(
    client: &reqwest::Client,
    prev: Option<&Bot>,
    name: &str,
    color: Color,
) -> FailureOr<Option<Bot>> {
    let status: RawBotStatus = {
        let mut status: RawBotStatus =
            json_get(client, &format!("/green/view/All/job/{}/api/json", name)).await?;
        status.builds.sort_by_key(|x| x.number);
        status
    };

    let last_build_id = match status.last_completed_build {
        Some(x) => x.number,
        None => {
            // If nothing's been done yet, just pretend the bot DNE. Not much else we can do,
            // really.
            return Ok(None);
        }
    };

    let last_first_failing: Option<&CompletedBuild>;
    if let Some(prev_state) = prev {
        if prev_state.status.most_recent_build.id == last_build_id {
            return Ok(Some(prev_state.clone()));
        }
        last_first_failing = prev_state.status.first_failing_build.as_ref();
    } else {
        last_first_failing = None;
    }

    let first_failing_build: Option<CompletedBuild>;
    match (status.last_successful_build, status.last_unsuccessful_build) {
        (None, None) => {
            warn!(
                "Bot {} had last build ID {}, but no successful/unsuccessful builds",
                name, last_build_id
            );
            return Ok(None);
        }
        (Some(_), None) => {
            first_failing_build = None;
        }
        (None, Some(u)) => {
            first_failing_build = Some(match last_first_failing {
                Some(x) => x.clone(),
                None => {
                    find_first_failing_build(client, name, &status.builds, None, u.number).await?
                }
            });
        }
        (Some(s), Some(u)) => {
            first_failing_build = if u.number > s.number {
                match last_first_failing {
                    Some(x) => Some(x.clone()),
                    None => Some(
                        find_first_failing_build(
                            client,
                            name,
                            &status.builds,
                            Some(s.number),
                            u.number,
                        )
                        .await?,
                    ),
                }
            } else {
                None
            };
        }
    };

    let most_recent_build = fetch_completed_build(client, name, last_build_id).await?;
    Ok(Some(Bot {
        // FIXME: GreenDragon has categories and quite a few bots. Maybe use their
        // categories, too?
        category: "GreenDragon".to_owned(),
        status: BotStatus {
            first_failing_build,
            most_recent_build,
            state: match color {
                Color::Disabled => BuilderState::Offline,
                Color::Red { flashing } => {
                    if flashing {
                        BuilderState::Building
                    } else {
                        BuilderState::Idle
                    }
                }
                Color::Blue { flashing } => {
                    if flashing {
                        BuilderState::Building
                    } else {
                        BuilderState::Idle
                    }
                }
            },
        },
    }))
}

pub(crate) async fn fetch_new_status_snapshot(
    client: &reqwest::Client,
    prev: &HashMap<String, Bot>,
) -> FailureOr<HashMap<String, Bot>> {
    let mut result = HashMap::new();

    // "All build groups" is necessary, since greendragon also includes a lot of miscellaneous
    // Apple-specific jobs (e.g., checking mac mini health/etc). Surfacing that probably isn't a
    // great idea.
    for bot in json_get::<AllBotsOverview>(client, "/green/view/All%20build%20groups/api/json")
        .await?
        .jobs
    {
        match fetch_single_bot_status_snapshot(client, prev.get(&bot.name), &bot.name, bot.color)
            .await?
        {
            None => {
                // OK then.
            }
            Some(x) => {
                result.insert(bot.name, x);
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
}

impl<'de> Deserialize<'de> for RawBuildResult {
    fn deserialize<D>(deserializer: D) -> Result<RawBuildResult, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = RawBuildResult;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "either SUCCESS, FAILURE, or ABORTED")
            }

            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match s {
                    "ABORTED" => Ok(RawBuildResult::Aborted),
                    "SUCCESS" => Ok(RawBuildResult::Success),
                    "FAILURE" => Ok(RawBuildResult::Failure),
                    _ => Err(E::custom(format!("{:?} isn't a valid RawBuildResult", s))),
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
    fn as_datetime(self) -> FailureOr<chrono::NaiveDateTime> {
        let millis = self.0 as i64;
        let secs = millis / 1000;
        let nanos = ((millis % 1000) * 1_000_000) as u32;
        match chrono::NaiveDateTime::from_timestamp_opt(secs, nanos) {
            Some(x) => Ok(x),
            None => bail!("invalid timestamp: {}", self.0),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChangeSet {
    author_email: String,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct ChangeSetListing {
    items: Vec<ChangeSet>,
}

#[derive(Deserialize)]
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

async fn fetch_completed_build(
    client: &reqwest::Client,
    bot_name: &str,
    id: BuildNumber,
) -> FailureOr<CompletedBuild> {
    let data: BuildResult =
        json_get(client, &format!("green/job/{}/{}/api/json", bot_name, id)).await?;

    let mut blamelist = Vec::new();
    let all_change_sets: Vec<ChangeSetListing>;
    if let Some(x) = data.change_set {
        all_change_sets = vec![x];
    } else {
        all_change_sets = data.change_sets;
    }
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
            RawBuildResult::Failure => BuildbotResult::Failure,
        },
        completion_time: data.timestamp.as_datetime()?,
        blamelist,
    })
}
