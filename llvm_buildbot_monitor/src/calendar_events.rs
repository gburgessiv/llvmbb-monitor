use std::sync::{Arc, Mutex};
use std::{collections::HashSet, time::Duration};

use anyhow::{Context, Result};
use calendar_check::CommunityEvent;
use chrono::{DateTime, Utc};
use log::{error, info, warn};
use tokio::sync::{broadcast, watch};

use crate::storage::Storage;

struct State {
    events: Vec<CommunityEvent>,
    refreshed_at: DateTime<Utc>,
}

async fn load_state(
    client_cache: &mut Option<reqwest::Client>,
    now: DateTime<Utc>,
) -> Result<State> {
    // TODO: Should this client be kept around for longer?
    let client = match &client_cache {
        Some(x) => x,
        None => {
            let c = reqwest::Client::builder()
                .build()
                .context("building reqwest client")?;
            client_cache.insert(c)
        }
    };
    // Fetch events from 2wks into the future; should be plenty.
    let events = calendar_check::fetch_near_llvm_calendar_office_hour_events(
        client,
        &now,
        Duration::from_secs(14 * 24 * 60 * 60),
    )
    .await?;
    Ok(State {
        events,
        refreshed_at: now,
    })
}

// This infloops rather than returning a failure. While this may seem undesirable, users of it
// would just infloop themselves anyway.
async fn load_state_with_retry() -> State {
    let mut client_cache = None;
    let mut backoff_secs = 2;
    let mut client_tries = 0;
    loop {
        match load_state(&mut client_cache, Utc::now()).await {
            Ok(s) => return s,
            Err(x) => {
                // Drop the client occasionally in case there's something weird about it causing
                // the errors.
                client_tries += 1;
                if client_tries >= 5 {
                    client_cache = None;
                    client_tries = 0;
                }
                error!(
                    "Failed loading calendar event state. Trying again in {backoff_secs:?}: {x}"
                );
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = std::cmp::max(10 * 60, backoff_secs * 2);
            }
        }
    }
}

fn calculate_ping_time(event: &CommunityEvent) -> DateTime<Utc> {
    let ping_mins = event.description_data.ping_duration_before_start_mins;
    event.start_time - Duration::from_secs(ping_mins as u64 * 60)
}

fn load_and_gc_previous_calendar_pings(
    storage: &Mutex<Storage>,
    state: &State,
) -> HashSet<Box<str>> {
    let mut previous_pings = match storage.lock().unwrap().load_all_sent_calendar_pings() {
        Ok(p) => p.into_iter().collect::<HashSet<String>>(),
        Err(x) => {
            error!("Error loading previous calendar pings; starting fresh: {x}");
            return Default::default();
        }
    };

    let mut result = HashSet::new();
    for event in &state.events {
        if let Some(s) = previous_pings.take(event.id.as_ref()) {
            result.insert(s.into());
        }
    }

    // Now do the GC part. Everything not in `events` should still be sitting in
    // `previous_pings`. Anything not in `events` probably won't appear again.
    //
    // Realistically, I could GC more often, but this process is restarted on
    // the server at least a few times a month, and we'll probably top out at a
    // few dozen pings per week.
    if !previous_pings.is_empty() {
        let storage = storage.lock().unwrap();
        for gc_ping in previous_pings {
            if let Err(x) = storage.remove_sent_calendar_ping(&gc_ping) {
                error!("Failed removing ping {gc_ping:?} from sqlite: {x}");
            }
        }
    }
    result
}

fn run_discord_pings(
    state: &Arc<State>,
    ping_indices: &[usize],
    storage: &Arc<Mutex<Storage>>,
    discord_messages: &broadcast::Sender<CommunityEvent>,
) {
    let events_to_ping = ping_indices.iter().map(|&i| (i, &state.events[i]));
    let mut successful_pings = Vec::new();
    for (i, event) in events_to_ping {
        if discord_messages.send(event.clone()).is_err() {
            warn!(
                "Can't send calendar ping for event {:?}; no receivers of the message exist",
                event.id
            );
            continue;
        }
        successful_pings.push(i);
    }

    if successful_pings.is_empty() {
        return;
    }

    let state = state.clone();
    let storage = storage.clone();
    // Just drop this on the floor, under the expectation that it'll
    // complete soon enough anyway.
    std::mem::drop(tokio::task::spawn_blocking(move || {
        for i in successful_pings {
            let id = state.events[i].id.as_ref();
            if let Err(x) = storage.lock().unwrap().add_sent_calendar_ping(id) {
                error!("Failed adding sent calendar ping to storage: {x}");
            }
        }
    }));
}

fn calculate_event_ping_data(
    state: &State,
    now: &DateTime<Utc>,
    already_pinged: &HashSet<Box<str>>,
) -> (Vec<usize>, Option<DateTime<Utc>>) {
    let pingable = state
        .events
        .iter()
        .enumerate()
        .filter(|(_, e)| e.start_time > *now && !already_pinged.contains(&e.id))
        .map(|(i, e)| (i, calculate_ping_time(e)));

    let mut ping_now_indices = Vec::new();
    let mut nearest_unfired_ping = None::<DateTime<Utc>>;
    for (i, ping_time) in pingable {
        if ping_time <= *now {
            ping_now_indices.push(i);
            continue;
        }

        match &nearest_unfired_ping {
            None => {
                nearest_unfired_ping = Some(ping_time);
            }
            Some(p) => {
                nearest_unfired_ping = Some(std::cmp::min(*p, ping_time));
            }
        }
    }
    (ping_now_indices, nearest_unfired_ping)
}

async fn run_discord_integration_forever(
    storage: Arc<Mutex<Storage>>,
    mut state_receiver: watch::Receiver<Arc<State>>,
    discord_messages: broadcast::Sender<CommunityEvent>,
) {
    let mut already_pinged = {
        let storage = storage.clone();
        let state = state_receiver.borrow().clone();
        tokio::task::spawn_blocking(move || load_and_gc_previous_calendar_pings(&storage, &state))
            .await
            .unwrap()
    };

    loop {
        let events = state_receiver.borrow().clone();
        let now = Utc::now();
        let (ping_now_indices, nearest_unfired_ping) =
            calculate_event_ping_data(&events, &now, &already_pinged);

        if !ping_now_indices.is_empty() {
            // While this hasn't been pinged _yet_, it will be very shortly
            // by the following `spawn`. Note this can't be done in the loop
            // above because the iterator makes immutable use of
            // `already_pinged`.
            for &i in &ping_now_indices {
                already_pinged.insert(events.events[i].id.clone());
            }
            run_discord_pings(&events, &ping_now_indices, &storage, &discord_messages);
        }

        match nearest_unfired_ping {
            None => {
                // If there's no ping to send, just wait merrily.
                state_receiver.changed().await.unwrap();
            }
            Some(ping_at) => {
                let time_to_ping = ping_at - now;
                // NOTE: This is unwrappable safely because the unfired ping
                // must be > now. Since now's a var instead of a syscall, it's
                // unchanging.
                let time_to_ping = time_to_ping.to_std().unwrap();
                // 'Cheap' version of select!ing between a tokio::time::timeout
                // and state_receiver.changed(); in either case, all there is to
                // do is loop again.
                let _ = tokio::time::timeout(time_to_ping, state_receiver.changed()).await;
            }
        }
    }
}

// Arbitrarily choose 2mins to refresh data before we ping, since the
// refresh will have to deal with network latency/potential retries.
const PRE_PING_STATE_REFRESH_TIME: Duration = Duration::from_secs(2 * 60);

fn calculate_next_refresh_time(state: &State) -> DateTime<Utc> {
    // Event refresh rules:
    //   1. Events should be refreshed at minimum once per hour.
    //   2. If an event has a ping coming up, events should be refreshed shortly before that
    //      ping to verify the event hasn't been removed or altered or whatever.
    let next_event_refresh = state
        .events
        .iter()
        .filter_map(|x| {
            let ping_at_time = calculate_ping_time(x);
            let refresh_at_time = ping_at_time - PRE_PING_STATE_REFRESH_TIME;
            if state.refreshed_at >= refresh_at_time {
                return None;
            }
            Some(refresh_at_time)
        })
        .min();

    let next_refresh_by_cadence = state.refreshed_at + Duration::from_secs(60 * 60);
    match next_event_refresh {
        None => next_refresh_by_cadence,
        Some(p) => std::cmp::min(p, next_refresh_by_cadence),
    }
}

pub(crate) async fn run_calendar_forever(
    storage: Arc<Mutex<Storage>>,
    discord_messages: broadcast::Sender<CommunityEvent>,
) {
    // N.B., this is Arc<> because `watch` has internal sync locking. The
    // discord integration may want to hold a borrow() for a while, which
    // could get spicy. Skip the spice by using a cheaply-cloneable type.
    let (state_sender, state_receiver) = watch::channel(Arc::new({
        let state = load_state_with_retry().await;
        info!(
            "Initial calendar check loaded {} events",
            state.events.len()
        );
        state
    }));

    tokio::spawn(run_discord_integration_forever(
        storage.clone(),
        state_receiver,
        discord_messages,
    ));
    loop {
        // N.B., This is the only sender, so a simple `.borrow()` here instead of
        // `.borrow().clone()` is fine.
        let next_refresh = calculate_next_refresh_time(&state_sender.borrow());
        info!(
            "Calendar refresh scheduling: waiting until {:?} for next refresh.",
            next_refresh
        );
        let sleep_dur = next_refresh - Utc::now();
        // If this returns Err, the time for sleeping has passed.
        if let Ok(sleep_dur) = sleep_dur.to_std() {
            tokio::time::sleep(sleep_dur).await;
        }

        let new_state = load_state_with_retry().await;
        info!(
            "Calendar refresh successfully loaded {:?} events",
            new_state.events.len()
        );
        // The receiver is meant to also run forever. Something really bad
        // happened if it died. Probably best to just crash the program at that
        // point, honestly.
        state_sender.send(Arc::new(new_state)).unwrap();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn arbitrary_time() -> DateTime<Utc> {
        DateTime::from_timestamp(1736002375, 0).unwrap()
    }

    fn to_already_pinged_set(s: &[&str]) -> HashSet<Box<str>> {
        s.iter().map(|x| (*x).into()).collect()
    }

    #[test]
    fn test_calculate_event_ping_data_ignores_already_pinged_or_started() {
        let baseline_time = arbitrary_time();
        let state = State {
            events: vec![
                CommunityEvent {
                    start_time: baseline_time - Duration::from_secs(1),
                    end_time: baseline_time + Duration::from_secs(360),
                    id: "1-sec-ago".into(),
                    description_data: calendar_check::CommunityEventDescriptionData {
                        ping_duration_before_start_mins: 0,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                CommunityEvent {
                    start_time: baseline_time,
                    end_time: baseline_time + Duration::from_secs(360),
                    id: "now".into(),
                    description_data: calendar_check::CommunityEventDescriptionData {
                        ping_duration_before_start_mins: 0,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                CommunityEvent {
                    start_time: baseline_time + Duration::from_secs(2),
                    end_time: baseline_time + Duration::from_secs(360),
                    id: "in-2-secs".into(),
                    description_data: calendar_check::CommunityEventDescriptionData {
                        ping_duration_before_start_mins: 0,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                CommunityEvent {
                    start_time: baseline_time + Duration::from_secs(5),
                    end_time: baseline_time + Duration::from_secs(360),
                    id: "in-5-secs".into(),
                    description_data: calendar_check::CommunityEventDescriptionData {
                        ping_duration_before_start_mins: 0,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            refreshed_at: baseline_time,
        };

        let now = baseline_time + Duration::from_secs(1);
        let (to_ping, next_ping_datetime) =
            calculate_event_ping_data(&state, &now, &to_already_pinged_set(&["in-2-secs"]));
        assert!(to_ping.is_empty(), "{to_ping:?}");
        assert_eq!(
            next_ping_datetime,
            Some(baseline_time + Duration::from_secs(5))
        );

        let (to_ping, next_ping_datetime) = calculate_event_ping_data(
            &state,
            &now,
            &to_already_pinged_set(&["in-2-secs", "in-5-secs"]),
        );
        assert!(to_ping.is_empty(), "{to_ping:?}");
        assert_eq!(next_ping_datetime, None);
    }

    #[test]
    fn test_calculate_event_ping_data_selects_nearest_event() {
        let baseline_time = arbitrary_time();
        let state = State {
            events: vec![
                CommunityEvent {
                    start_time: baseline_time,
                    end_time: baseline_time + Duration::from_secs(360),
                    id: "now".into(),
                    description_data: calendar_check::CommunityEventDescriptionData {
                        ping_duration_before_start_mins: 0,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                CommunityEvent {
                    start_time: baseline_time + Duration::from_secs(2),
                    end_time: baseline_time + Duration::from_secs(360),
                    id: "in-2-secs".into(),
                    description_data: calendar_check::CommunityEventDescriptionData {
                        ping_duration_before_start_mins: 0,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                CommunityEvent {
                    start_time: baseline_time + Duration::from_secs(5),
                    end_time: baseline_time + Duration::from_secs(360),
                    id: "in-5-secs".into(),
                    description_data: calendar_check::CommunityEventDescriptionData {
                        ping_duration_before_start_mins: 0,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            refreshed_at: baseline_time,
        };

        let now = baseline_time;
        let (to_ping, next_ping_datetime) =
            calculate_event_ping_data(&state, &now, &Default::default());
        assert!(to_ping.is_empty(), "{to_ping:?}");
        assert_eq!(
            next_ping_datetime,
            Some(baseline_time + Duration::from_secs(2))
        );
    }

    #[test]
    fn test_calculate_event_ping_data_pings_all_unstarted() {
        let baseline_time = arbitrary_time();
        let state = State {
            events: vec![
                CommunityEvent {
                    start_time: baseline_time,
                    end_time: baseline_time + Duration::from_secs(360),
                    id: "now".into(),
                    description_data: calendar_check::CommunityEventDescriptionData {
                        ping_duration_before_start_mins: 0,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                CommunityEvent {
                    start_time: baseline_time + Duration::from_secs(2),
                    end_time: baseline_time + Duration::from_secs(360),
                    id: "in-2-secs".into(),
                    description_data: calendar_check::CommunityEventDescriptionData {
                        ping_duration_before_start_mins: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                CommunityEvent {
                    start_time: baseline_time + Duration::from_secs(5),
                    end_time: baseline_time + Duration::from_secs(360),
                    id: "in-5-secs".into(),
                    description_data: calendar_check::CommunityEventDescriptionData {
                        ping_duration_before_start_mins: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                CommunityEvent {
                    start_time: baseline_time + Duration::from_secs(10 + 60),
                    end_time: baseline_time + Duration::from_secs(360),
                    id: "in-65-secs".into(),
                    description_data: calendar_check::CommunityEventDescriptionData {
                        ping_duration_before_start_mins: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            refreshed_at: baseline_time,
        };

        let now = baseline_time;
        let (to_ping, next_ping_datetime) =
            calculate_event_ping_data(&state, &now, &Default::default());
        assert_eq!(&to_ping, &[1, 2]);
        assert_eq!(
            next_ping_datetime,
            Some(baseline_time + Duration::from_secs(10))
        );
    }

    #[test]
    fn test_state_refresh_picks_an_hour_if_no_near_events() {
        let baseline_time = arbitrary_time();
        let state = State {
            events: Vec::new(),
            refreshed_at: baseline_time - Duration::from_secs(5),
        };
        assert_eq!(
            calculate_next_refresh_time(&state),
            baseline_time + Duration::from_secs(60 * 60 - 5)
        );

        let state = State {
            events: vec![CommunityEvent {
                start_time: baseline_time + Duration::from_secs(60 * 60 * 2),
                end_time: baseline_time + Duration::from_secs(60 * 60 * 3),
                description_data: calendar_check::CommunityEventDescriptionData {
                    ping_duration_before_start_mins: 0,
                    ..Default::default()
                },
                ..Default::default()
            }],
            refreshed_at: baseline_time - Duration::from_secs(5),
        };
        assert_eq!(
            calculate_next_refresh_time(&state),
            baseline_time + Duration::from_secs(60 * 60 - 5)
        );
    }

    #[test]
    fn test_state_refresh_picks_nearest_unstarted_event_for_refresh() {
        let baseline_time = arbitrary_time();
        let state = State {
            events: vec![
                CommunityEvent {
                    start_time: baseline_time - Duration::from_secs(1),
                    end_time: baseline_time + Duration::from_secs(3600),
                    id: "1-sec-ago".into(),
                    description_data: calendar_check::CommunityEventDescriptionData {
                        ping_duration_before_start_mins: 0,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                CommunityEvent {
                    start_time: baseline_time + Duration::from_secs(2 * 60),
                    end_time: baseline_time + Duration::from_secs(3600),
                    id: "in-2-minutes".into(),
                    description_data: calendar_check::CommunityEventDescriptionData {
                        ping_duration_before_start_mins: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                CommunityEvent {
                    start_time: baseline_time + Duration::from_secs(10 * 60),
                    end_time: baseline_time + Duration::from_secs(3600),
                    id: "in-10-minutes".into(),
                    description_data: calendar_check::CommunityEventDescriptionData {
                        ping_duration_before_start_mins: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                CommunityEvent {
                    start_time: baseline_time + Duration::from_secs(20 * 60),
                    end_time: baseline_time + Duration::from_secs(3600),
                    id: "in-20-minutes".into(),
                    description_data: calendar_check::CommunityEventDescriptionData {
                        ping_duration_before_start_mins: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            refreshed_at: baseline_time,
        };

        assert_eq!(
            calculate_next_refresh_time(&state),
            // This is a selection of `in-10-minutes`:
            // - 1-sec-ago and in-2-minutes have already passed. The latter is because we want a
            //   ping in 1 minute, and the state was refreshed 1min before the bing is desired
            //   (refresh time is currently 2min).
            // - in-20-minutes needs a refresh farther out than in-10-minutes.
            baseline_time + Duration::from_secs(9 * 60) - PRE_PING_STATE_REFRESH_TIME,
        );
    }
}
