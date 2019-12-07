use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::{Duration, Instant};

use log::{error, info};
use serde::Deserialize;

type ErrorOr<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
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
    #[serde(rename = "basedir")]
    base_dir: String,
    // Always sorted in increasing order.
    cached_builds: Vec<BuildNumber>,
    // Always sorted in increasing order.
    #[serde(default)]
    current_builds: Vec<BuildNumber>,
    pending_builds: u32,
    // Always sorted in lexicographically increasing order.
    slaves: Vec<String>,
    state: BuilderState,
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

    /*
    async fn fetch_build_status(&self, builder: &str, n: BuildNumber) -> FailureOr<Build> {
        Ok(self.json_get("/json/builders").await?)
    }
    */
}

/*
#[derive(Clone, Debug)]
enum BuildStatus {
    Exception,
    Failure,
    Success,
}

#[derive(Clone, Debug)]
struct Build {
    number: BuildNumber,
    status: BuildStatus,
}
*/

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum BotEvent {
    // The Vec<BuildNumber> is every event in the bot's (cached) history.
    NewBot(String, Vec<BuildNumber>),
    NewBuilds(String, Vec<BuildNumber>),
    RemovedBot(String),
}

async fn find_bot_status_deltas(
    last_latest_builds: &Option<HashMap<String, BuildNumber>>,
    client: &LLVMLabClient,
) -> FailureOr<(Vec<BotEvent>, HashMap<String, BuildNumber>)> {
    let bot_statuses = client.fetch_full_builder_status().await?;

    let mut new_latest_builds: HashMap<String, BuildNumber> = HashMap::new();
    let mut events: Vec<BotEvent> = Vec::new();
    let mut unmentioned_bots: HashSet<&String>;
    if let Some(latest) = last_latest_builds {
        unmentioned_bots = latest.keys().collect();
    } else {
        unmentioned_bots = HashSet::new();
    };

    for (bot_name, status) in bot_statuses.into_iter() {
        // If it has no builds, it's effectively dead to us.
        let latest_build = match status.cached_builds.iter().max() {
            Some(x) => *x,
            None => continue,
        };

        unmentioned_bots.remove(&bot_name);
        if let Some(ref l) = last_latest_builds {
            if let Some(latest_build) = l.get(&bot_name) {
                let running_builds: HashSet<_> = status.current_builds.iter().map(|x| *x).collect();
                let new_builds: Vec<_> = status
                    .cached_builds
                    .iter()
                    .map(|x| *x)
                    .filter(|x| x > latest_build && !running_builds.contains(x))
                    .collect();
                if !new_builds.is_empty() {
                    events.push(BotEvent::NewBuilds(bot_name.clone(), new_builds));
                }
            } else {
                events.push(BotEvent::NewBot(bot_name.clone(), status.cached_builds));
            }
        } else {
            events.push(BotEvent::NewBot(bot_name.clone(), status.cached_builds));
        }

        new_latest_builds.insert(bot_name, latest_build);
    }

    for bot in unmentioned_bots {
        events.push(BotEvent::RemovedBot(bot.clone()));
    }
    Ok((events, new_latest_builds))
}

// Use an unbounded channel because honestly, this is going to get a single message per minute,
// tops. If we OOM, something was beyond seriously wrong.
async fn bot_event_consumer(mut stream: tokio::sync::mpsc::UnboundedReceiver<Vec<BotEvent>>) {
    while let Some(event_list) = stream.recv().await {
        for event in event_list {
            match event {
                BotEvent::NewBot(bot_name, _) => {
                    info!("New bot added: {}", bot_name);
                }
                BotEvent::NewBuilds(bot_name, builds) => {
                    info!("New build(s) detected: {}/{:?}", bot_name, builds);
                }
                BotEvent::RemovedBot(bot_name) => {
                    info!("Bot removed: {}", bot_name);
                }
            }
        }
    }
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

#[tokio::main]
async fn main() -> ErrorOr<()> {
    simple_logger::init_with_level(log::Level::Info).unwrap();
    let client = LLVMLabClient::new("http://lab.llvm.org:8011")?;

    let (mut event_sender, event_receiver) = tokio::sync::mpsc::unbounded_channel();

    tokio::spawn(bot_event_consumer(event_receiver));
    let mut last_latest_builds = None;
    // FIXME: 5min -> 1min. Rn, since this isn't useful to anyone but me, I'm doing 5min.
    let mut ticks = new_async_ticker(Duration::from_secs(5 * 60));
    while let Some(_) = ticks.recv().await {
        match find_bot_status_deltas(&last_latest_builds, &client).await {
            Ok((mut deltas, new_state)) => {
                last_latest_builds = Some(new_state);
                if !deltas.is_empty() {
                    // Makes downstream output marginally prettier.
                    deltas.sort();
                    info!("{} changes in buildbots", deltas.len());
                    event_sender
                        .try_send(deltas)
                        .expect("Other end should never close before us");
                } else {
                    info!("No changes in buildbots");
                }
            }
            Err(x) => {
                error!("Updating bot statuses failed: {}", x);
            }
        };
    }
    Ok(())
}
