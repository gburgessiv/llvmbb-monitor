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
use log::warn;
use serde::Deserialize;

#[derive(Deserialize)]
struct UnabridgedBuildStatus;

#[derive(Deserialize)]
struct UnabridgedBuilderStatus;

lazy_static! {
    static ref HOST: reqwest::Url =
        reqwest::Url::parse("http://green.lab.llvm.org").expect("parsing greendragon URL");
}

macro_rules! try_with_context {
    ($x:expr, $s:expr, $($xs:expr),*) => {{
        match $x {
            Ok(y) => y,
            Err(x) => {
                let mut msg = format!($s, $($xs),*);
                msg += ": ";

                use std::fmt::Write;
                msg.write_fmt(format_args!("{}", x)).unwrap();
                let x: failure::Error = x.into();
                return Err(x.context(msg).into());
            }
        }
    }}
}

async fn json_get<T>(client: &reqwest::Client, path: &str) -> FailureOr<T>
where
    T: serde::de::DeserializeOwned,
{
    let resp = try_with_context!(
        client.get(HOST.join(path)?).send().await,
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

// http://green.lab.llvm.org/green/view/All/job/clang-stage1-cmake-RA-incremental/api/json?pretty=true

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
    let status: RawBotStatus =
        json_get(client, &format!("/green/view/All/job/{}/api/json", name)).await?;

    let last_build_id = match status.last_completed_build {
        Some(x) => x.number,
        None => {
            // If nothing's been done yet, just pretend the bot DNE. Not much else we can do,
            // really.
            return Ok(None);
        }
    };

    let last_failing: Option<&CompletedBuild>;
    if let Some(prev_state) = prev {
        if prev_state.status.most_recent_build.id == last_build_id {
            return Ok(Some(prev_state.clone()));
        }
        last_failing = prev_state.status.first_failing_build.as_ref();
    } else {
        last_failing = None;
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
            first_failing_build = Some(match last_failing {
                Some(x) => x.clone(),
                None => fetch_completed_build(client, name, u.number).await?,
            });
        }
        (Some(s), Some(u)) => {
            first_failing_build = if u.number > s.number {
                match last_failing {
                    Some(x) => Some(x.clone()),
                    None => Some(fetch_completed_build(client, name, u.number).await?),
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
            first_failing_build: first_failing_build,
            most_recent_build: most_recent_build,
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
    #[serde(default)]
    change_set: ChangeSetListing,
}

async fn fetch_completed_build(
    client: &reqwest::Client,
    bot_name: &str,
    id: BuildNumber,
) -> FailureOr<CompletedBuild> {
    let data: BuildResult =
        json_get(client, &format!("green/job/{}/{}/api/json?pretty=true", bot_name, id)).await?;

    let mut blamelist = Vec::new();
    for change_set in data.change_set.items.into_iter() {
        match Email::parse(&change_set.author_email) {
            Some(x) => blamelist.push(x),
            None => warn!("Unparseable email: {:?}", &change_set.author_email),
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
        blamelist: blamelist,
    })
}
