// TODO: VERIFY THAT THE TIMEZONE STUFF WORKS HERE - CURRENTLY SET TO UTC BUT COULD REQUIRE SPECIAL
// TREATMENT
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use log::{debug, warn};
use serde::de::Deserializer;
use serde::{Deserialize, Serialize};

type UtcTime = chrono::DateTime<chrono::Utc>;

// "Must be an RFC3339 timestamp with mandatory time zone offset, for example,
// 2011-06-03T10:00:00-07:00, 2011-06-03T10:00:00Z. Milliseconds may be provided but are ignored."
fn utc_time_to_api_time(time: &UtcTime) -> String {
    let use_z = true;
    time.to_rfc3339_opts(chrono::SecondsFormat::Secs, use_z)
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum CommunityEventType {
    OfficeHours,
    SyncUp,
}

#[derive(Serialize)]
struct CommunityEventDescriptionData {
    event_type: CommunityEventType,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    mention_users: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    mention_channels: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ping_duration_before_start_mins: Option<u32>,
}

#[derive(Serialize)]
struct CommunityEvent {
    // Unix millis.
    start_time_unix: u64,
    end_time_unix: u64,
    title: String,

    #[serde(flatten)]
    description_data: CommunityEventDescriptionData,
}

fn parse_event_description_data(event_description: &str) -> Option<CommunityEventDescriptionData> {
    let mut event_type = None;
    let mut event_channels = None;
    let mut event_mention = None;
    let mut event_reminder = None;

    for line in event_description.lines() {
        if let Some(line) = strip_prefix_once(&event_type, "discord-bot-event-type:", line) {
            let line = line.trim();
            let e = match line {
                "office-hours" => CommunityEventType::OfficeHours,
                "sync-up" => CommunityEventType::SyncUp,
                _ => {
                    warn!("Ignoring event with invalid event-type: {line:?}");
                    return None;
                }
            };
            event_type = Some(e);
            continue;
        }

        if let Some(line) =
            strip_prefix_once(&event_channels, "discord-bot-channels-to-mention:", line)
        {
            let mut channels = Vec::new();
            for channel in line.split(',') {
                let channel = channel.trim();
                if channel.starts_with('#') {
                    channels.push(channel.into());
                } else {
                    warn!("Dropping channel without leading #: {channel:?}");
                }
            }
            event_channels = Some(channels);
            continue;
        }

        if let Some(line) = strip_prefix_once(&event_mention, "discord-bot-mention:", line) {
            let mut users = Vec::new();
            for user in line.split(',') {
                let user = user.trim();
                if user.starts_with('@') {
                    users.push(user.into());
                } else {
                    warn!("Dropping user without leading @: {user:?}");
                }
            }
            event_mention = Some(users);
            continue;
        }

        if let Some(line) = strip_prefix_once(
            &event_reminder,
            "discord-bot-reminder-minutes-before-start:",
            line,
        ) {
            let line = line.trim();
            match line.parse() {
                Ok(n) => {
                    event_reminder = Some(n);
                }
                Err(x) => {
                    warn!("Failed parsing start-before time of {line:?}: {x}");
                }
            }
            continue;
        }
    }

    let Some(event_type) = event_type else {
        if event_channels.is_some() || event_mention.is_some() || event_reminder.is_some() {
            warn!("Skipping event with no event-type, but other discord bot metadata");
        }

        return None;
    };

    return Some(CommunityEventDescriptionData {
        event_type,
        mention_users: event_mention.unwrap_or_default(),
        mention_channels: event_channels.unwrap_or_default(),
        ping_duration_before_start_mins: event_reminder,
    });

    fn strip_prefix_once<'a, T>(
        existing_value: &Option<T>,
        prefix: &str,
        strip_from: &'a str,
    ) -> Option<&'a str> {
        let no_prefix = strip_from.strip_prefix(prefix)?;
        if existing_value.is_some() {
            warn!("Discarding extra {prefix:?} directive; one was already parsed.");
            return None;
        }
        Some(no_prefix)
    }
}

struct GoogleCalendarDateTime(UtcTime);

impl<'de> Deserialize<'de> for GoogleCalendarDateTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let datetime =
            chrono::DateTime::parse_from_rfc3339(&s).map_err(serde::de::Error::custom)?;
        Ok(Self(datetime.to_utc()))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoogleCalendarEventTime {
    date_time: Option<GoogleCalendarDateTime>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoogleCalendarEvent {
    description: Option<String>,
    end: Option<GoogleCalendarEventTime>,
    start: Option<GoogleCalendarEventTime>,
    summary: String,
}

fn convert_cal_events_to_office_hours(cal_events: Vec<GoogleCalendarEvent>) -> Vec<CommunityEvent> {
    let mut results = Vec::new();
    for event in cal_events {
        debug!("Checking event with title: {}", event.summary);
        let Some(start_time) = event.start.and_then(|x| event_time_to_timestamp(&x)) else {
            debug!("Skip: no start_time");
            continue;
        };

        let Some(end_time) = event.end.and_then(|x| event_time_to_timestamp(&x)) else {
            debug!("Skip: no end_time");
            continue;
        };

        let Some(event_description) = event.description else {
            debug!("Skip: no description");
            continue;
        };

        let Some(description_data) = parse_event_description_data(&event_description) else {
            debug!("Skip: no community event data in description");
            continue;
        };

        results.push(CommunityEvent {
            start_time_unix: start_time,
            end_time_unix: end_time,
            title: event.summary,
            description_data,
        });
    }
    return results;

    fn event_time_to_timestamp(event_time: &GoogleCalendarEventTime) -> Option<u64> {
        // For all-day events, the `date` field will be set. We don't have all-day office hours.
        let event_date_time = event_time.date_time.as_ref()?;
        let timestamp = event_date_time.0.timestamp();
        let Ok(result) = timestamp.try_into() else {
            warn!("Invalid event date-time encountered: {timestamp}");
            return None;
        };
        Some(result)
    }
}

async fn fetch_public_google_calendar_json(
    start_time: &UtcTime,
    end_time: &UtcTime,
) -> Result<String> {
    // TODO: This should really loop with a page token, but:
    // 1. I'm lazy.
    // 2. This is requesting 2500 events - the max - within (realistically) under a month.
    // 3. The events are ordered by start time
    //
    // ...Soooo, pretty unlikely looping will lead to a functional difference.

    // This query has an embedded token:
    // https://clients6.google.com/calendar/v3/calendars/calendar%40llvm.org/events?calendarId=calendar%40llvm.org&singleEvents=true&eventTypes=default&eventTypes=focusTime&eventTypes=outOfOffice&timeZone=GMT-05%3A00&maxAttendees=1&maxResults=250&sanitizeHtml=true&timeMin=2024-12-01T00%3A00%3A00%2B18%3A00&timeMax=2025-01-05T00%3A00%3A00-18%3A00&key=AIzaSyBNlYH01_9Hc5S1J9vuFmu2nUqBZJNAXxs&%24unique=gc456
    // Searching for it shows results from many years ago (2018). It's also hard-coded as a
    // constant in Calendar's embedded JS. Rather than dealing with token recycling and everything,
    // just embed it and hope that I can continue to be lazy.
    let mut calendar_url = reqwest::Url::parse(
        "https://clients6.google.com/calendar/v3/calendars/calendar%40llvm.org/events",
    )
    .unwrap();

    calendar_url
        .query_pairs_mut()
        .append_pair("calendarId", "calendar@llvm.org")
        .append_pair("eventTypes", "default")
        .append_pair("eventTypes", "focusTime")
        .append_pair("eventTypes", "outOfOffice")
        .append_pair("key", "AIzaSyBNlYH01_9Hc5S1J9vuFmu2nUqBZJNAXxs")
        .append_pair("maxAttendees", "500") // 500 is the max supported by the API.
        .append_pair("maxResults", "2500") // 2500 is the max supported by the API.
        .append_pair("orderBy", "startTime")
        .append_pair("sanitizeHtml", "true")
        .append_pair("singleEvents", "true")
        .append_pair("timeMax", &utc_time_to_api_time(end_time))
        .append_pair("timeMin", &utc_time_to_api_time(start_time))
        .append_pair("timeZone", "UTC")
        .append_pair("unique", "gc456");

    debug!("Fetching {calendar_url:?}");
    let response = reqwest::get(calendar_url)
        .await?
        .error_for_status()
        .context("getting LLVM calendar")?;
    let body_text = response.text().await.context("reading response body")?;
    Ok(body_text)
}

async fn fetch_near_llvm_calendar_office_hour_events(
    now: &UtcTime,
    max_distance: Duration,
) -> Result<Vec<CommunityEvent>> {
    let json = fetch_public_google_calendar_json(now, &(*now + max_distance))
        .await
        .context("fetching JSON")?;

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Response {
        items: Vec<GoogleCalendarEvent>,
    }

    let response =
        serde_json::from_str::<Response>(&json).context("decoding google calendar json")?;
    Ok(convert_cal_events_to_office_hours(response.items))
}

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

    let events = fetch_near_llvm_calendar_office_hour_events(
        &chrono::Utc::now(),
        // Fetch a few weeks out. Doubt anyone will care about more than that.
        Duration::from_secs(14 * 24 * 60 * 60),
    )
    .await
    .context("fetching LLVM office hour events")?;

    let output = if args.debug {
        serde_json::to_string_pretty(&events)
    } else {
        serde_json::to_string(&events)
    }
    .context("serializing events")?;

    println!("{}", output);
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    run(args).await
}
