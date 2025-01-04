use std::sync::Arc;
use std::{collections::HashSet,  time::Duration};

use anyhow::{Context, Result};
use calendar_check::CommunityEvent;
use chrono::{DateTime, Utc};
use log::{error, info};
use tokio::sync::watch;

struct State {
    events: Vec<CommunityEvent>,
    refreshed_at: DateTime<Utc>,
}

async fn load_state(now: DateTime<Utc>) -> Result<State> {
    // TODO: Should this client be kept around?
    let reqwest_client = reqwest::Client::builder()
        .build()
        .context("building reqwest client")?;

    // Fetch events from 2wks into the future; should be plenty.
    let events = calendar_check::fetch_near_llvm_calendar_office_hour_events(
        &reqwest_client,
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
    let mut backoff_secs = 2;
    loop {
        match load_state(Utc::now()).await {
            Ok(s) => return s,
            Err(x) => {
                error!(
                    "Failed loading calendar event state. Trying again in {backoff_secs:?}: {x}"
                );
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = std::cmp::max(10 * 60, backoff_secs * 2);
            }
        }
    }
}

fn calculate_ping_time(event: &CommunityEvent) -> Option<DateTime<Utc>> {
    let ping_mins = event.description_data.ping_duration_before_start_mins?;
    let ping_at_time = event.start_time - Duration::from_secs(ping_mins as u64 * 60);
    Some(ping_at_time)
}

async fn run_discord_pings(state: Arc<State>, ping_indices: Vec<usize>) {
    todo!();
}

async fn run_discord_integration_forever(mut state_receiver: watch::Receiver<Arc<State>>) {
    // TODO: Loading this from storage would be good; keeps the bot from
    // multi-pinging if it restarts (due to new docker container, machine
    // rebooting, bugs leading to crashes, ...)
    let mut already_pinged = HashSet::<Box<str>>::new();
    loop {
        let events = state_receiver.borrow().clone();
        let now = Utc::now();
        let pingable = events
            .events
            .iter()
            .enumerate()
            .filter(|(_, e)| e.start_time > now && !already_pinged.contains(&e.id))
            .filter_map(|(i, e)| {
                let ping_time = calculate_ping_time(e)?;
                Some((i, ping_time))
            });

        let mut ping_now_indices = Vec::new();
        let mut nearest_unfired_ping = None::<DateTime<Utc>>;
        for (i, ping_time) in pingable {
            if ping_time <= now {
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

        if !ping_now_indices.is_empty() {
            // While this hasn't been pinged _yet_, it will be very shortly
            // by the following `spawn`. Note this can't be done in the loop
            // above because the iterator makes immutable use of
            // `already_pinged`.
            for &i in &ping_now_indices {
                already_pinged.insert(events.events[i].id.clone());
            }
            tokio::spawn(run_discord_pings(events, ping_now_indices));
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

pub(crate) async fn run_calendar_forever() {
    // Arbitrarily choose 2mins to refresh data before we ping, since the
    // refresh will have to deal with network latency/potential retries.
    const PRE_PING_REFRESH_TIME: Duration = Duration::from_secs(2 * 60);

    // N.B., this is Arc<> because `watch` has internal sync locking. The
    // discord integration may want to hold a borrow() for a while, which
    // could get spicy. Skip the spice by using a cheaply-cloneable type.
    let (state_sender, state_receiver) = watch::channel(Arc::new(load_state_with_retry().await));

    tokio::spawn(run_discord_integration_forever(state_receiver));
    loop {
        // Event refresh rules:
        //   1. Events should be refreshed at minimum once per hour.
        //   2. If an event has a ping coming up, events should be refreshed shortly before that
        //      ping to verify the event hasn't been removed or altered or whatever.
        let (last_refresh, next_event_refresh) = {
            let state = state_sender.borrow();
            let next_event_refresh = state
                .events
                .iter()
                .filter_map(|x| {
                    let ping_at_time = calculate_ping_time(x)?;
                    let refresh_at_time = ping_at_time - PRE_PING_REFRESH_TIME;
                    if state.refreshed_at >= refresh_at_time {
                        return None;
                    }
                    Some(refresh_at_time)
                })
                .min();
            (state.refreshed_at, next_event_refresh)
        };

        let next_refresh_by_cadence = last_refresh + Duration::from_secs(60 * 60);
        let next_refresh = match next_event_refresh {
            None => next_refresh_by_cadence,
            Some(p) => std::cmp::min(p, next_refresh_by_cadence),
        };
        info!(
            concat!(
                "scheduling: last state refresh was {:?}, and next event ping is {:?}; ",
                "planning next refresh at {:?} (or now, if that's passed)",
            ),
            last_refresh, next_event_refresh, next_refresh,
        );

        let sleep_dur = next_refresh - Utc::now();
        // If this returns Err, the time for sleeping has passed.
        if let Ok(sleep_dur) = sleep_dur.to_std() {
            tokio::time::sleep(sleep_dur).await;
        }

        let new_state = load_state_with_retry().await;
        // The receiver is meant to also run forever. Something really bad
        // happened if it died. Probably best to just crash the program at that
        // point, honestly.
        state_sender.send(Arc::new(new_state)).unwrap();
    }
}
