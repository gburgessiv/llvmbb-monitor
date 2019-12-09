use crate::Bot;
use crate::BotStatusSnapshot;
use crate::BuilderState;
use crate::FailureOr;

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::sync::{Arc, Mutex};

use log::{error, info, warn};
use serenity::http::raw::Http;
use serenity::model::prelude::*;
use serenity::prelude::*;
use tokio::runtime::TaskExecutor;
use tokio::sync::watch;

struct PubsubData<T> {
    data: Option<T>,
    version: u32,
}

struct Pubsub<T>
where
    T: Clone,
{
    data: Mutex<PubsubData<T>>,
    cond: std::sync::Condvar,
}

impl<T> Pubsub<T>
where
    T: Clone,
{
    fn new() -> Arc<Self> {
        Arc::new(Self {
            data: Mutex::new(PubsubData {
                data: None,
                version: 0,
            }),
            cond: Default::default(),
        })
    }

    fn publish(&self, value: T) {
        let mut data = self.data.lock().unwrap();
        data.data = Some(value);
        data.version += 1;
        self.cond.notify_all();
    }

    fn reader(me: &Arc<Self>) -> PubsubReader<T> {
        PubsubReader {
            pubsub: me.clone(),
            version: 0,
        }
    }
}

struct PubsubReader<T>
where
    T: Clone,
{
    pubsub: Arc<Pubsub<T>>,
    version: u32,
}

impl<T> PubsubReader<T>
where
    T: Clone,
{
    fn next(&mut self) -> T {
        let mut data = self.pubsub.data.lock().unwrap();
        while data.version == self.version {
            data = self.pubsub.cond.wait(data).unwrap();
        }
        self.version = data.version;
        data.data.as_ref().unwrap().clone()
    }
}

/// So we get 0 guarantees about the relative ordering of
/// guild_create/guild_unavailable/guild_delete. All of those events are simply yeeted to our
/// threadpool.
///
/// So we have refcounts. These refcounts can go negative. It's great.
///
/// Grumbling aside, the core idea is that it's up to the server for a channel to periodically poll
/// its refcount. If its refcount is <= 0, it should remove itself from the refcounts and exit
/// immediately.
///
/// Refcounts can go negative with everyone's favorite racing GuildUnavailable <=> GuildCreate
/// spam. If a GuildUnavailable arrives before the GuildCreate that was intended to preceed it, ...
///
/// The important property here is that we never try to make the thread serving the guild fall
/// over, or wait for it to do so. In a GuildCreate/GuildUnavailable storm, that's Very Desirable.
/// Plus, since these threads should serve the same function (a new one just adds in another
/// channel setup phase), 'missing' a shutdown in such conditions isn't problematic.
///
/// Also, sharp edges: refcounts aren't atomic members stashed somewhere because that's more subtle
/// than this; you have to check the atomic, then if it's bad, come in here and check again
/// (otherwise if the atomic bumps to > 0 before you get to remove yourself from the hashmap, ...).
/// This is code that's incredibly unlikely to be exercised meaningfully but once in a blue moon.
/// Subtlety is expensive.
#[derive(Default)]
struct GuildServerState {
    /// Refcounts for live servers.
    ///
    /// If orphan_refcounts has a GuildId, this shall not have a matching one.
    server_refcounts: HashMap<GuildId, isize>,

    /// If a server exited with a negative refcount, this will contain that refcount negated.
    /// GuildCreates should not proceed to create a handler until the value here is 0 (or not
    /// present). Similarly, if we get a decref for a server that isn't in server_refcounts, this
    /// gets bumped.
    ///
    /// If server_refcounts has a GuildId, this shall not have a matching one.
    orphan_refcounts: HashMap<GuildId, usize>,
}

fn serve_channel<F>(
    http: &Http,
    channel_id: ChannelId,
    ui: &mut PubsubReader<Arc<UI>>,
    should_exit: &mut F,
) -> FailureOr<()>
where
    F: FnMut() -> bool,
{
    // First, delete all of the messages in the channel.
    info!("Removing existing messages in {}", channel_id);
    loop {
        let messages = channel_id.messages(http, |retriever| {
            // 100 is a discord limitation, sadly
            retriever.after(MessageId(0)).limit(100)
        })?;

        if messages.is_empty() {
            break;
        }

        channel_id.delete_messages(http, messages.iter().map(|x| x.id))?;
    }

    let mut current_ui;
    {
        let setup_message = channel_id.send_message(http, |m| {
            m.content(":man_construction_worker: one moment -- set-up is in progress... :woman_construction_worker:")
        })?;

        current_ui = ui.next();
        channel_id.delete_message(http, setup_message.id)?;
    }

    struct UIMessage {
        last_value: String,
        id: MessageId,
    }

    let mut existing_messages: Vec<UIMessage> = Vec::new();
    loop {
        let mut sent_message = false;
        for (i, data) in current_ui.messages.iter().enumerate() {
            if let Some(prev_data) = existing_messages.get_mut(i) {
                if *data == prev_data.last_value {
                    continue;
                }

                channel_id.edit_message(http, prev_data.id, |m| m.content(&*data))?;

                prev_data.last_value = data.clone();
                continue;
            }

            let discord_message = channel_id.send_message(http, |m| m.content(&*data))?;
            sent_message = true;
            existing_messages.push(UIMessage {
                last_value: data.clone(),
                id: discord_message.id,
            });
        }

        if current_ui.force_ping_after_refresh && !sent_message {
            let ping_message = channel_id.send_message(http, |m| m.content("friendly ping"))?;
            channel_id.delete_message(http, ping_message.id)?;
        }

        if should_exit() {
            return Ok(());
        }

        current_ui = ui.next();
    }
}

struct MessageHandler {
    pubsub: Arc<Pubsub<Arc<UI>>>,
    // The readonly distribution here makes an RwMutex more appropriate in theory, but there're
    // going to be two threads polling this ~minutely. So let's prefer simpler code.
    servers: Arc<Mutex<GuildServerState>>,
}

impl MessageHandler {
    fn decref_guild_server(&self, id: GuildId) {
        let mut servers = self.servers.lock().unwrap();
        match servers.server_refcounts.get_mut(&id) {
            Some(x) => {
                *x -= 1;
            }
            None => {
                *servers.orphan_refcounts.entry(id).or_default() += 1;
            }
        }
    }
}

impl serenity::client::EventHandler for MessageHandler {
    fn guild_create(&self, ctx: Context, guild: Guild, _is_new: bool) {
        info!("Guild #{} ({}) has been created", guild.id, guild.name);

        let guild_id = guild.id;
        {
            let mut servers = self.servers.lock().unwrap();
            if let Entry::Occupied(mut x) = servers.orphan_refcounts.entry(guild_id) {
                *x.get_mut() -= 1;
                if *x.get() == 0 {
                    x.remove();
                }
                return;
            }

            match servers.server_refcounts.entry(guild_id) {
                Entry::Occupied(mut x) => {
                    *x.get_mut() += 1;
                    return;
                }
                Entry::Vacant(x) => {
                    x.insert(1);
                }
            }
        }

        let my_channel_name = "llvmbb";
        let my_channel_id: ChannelId = match guild
            .channels
            .iter()
            .filter(|x| x.1.read().name == my_channel_name)
            .map(|x| x.0)
            .next()
        {
            Some(x) => {
                info!("Identified #{} as my channel in #{}", x, guild_id);
                *x
            }
            None => {
                error!("No {} channel in #{}; quit", my_channel_name, guild_id);
                return;
            }
        };

        let http = ctx.http;
        let mut pubsub_reader = Pubsub::reader(&self.pubsub);
        let guild_state = self.servers.clone();
        std::thread::spawn(move || {
            let mut responded_with_yes = false;
            let mut should_exit = move || {
                if responded_with_yes {
                    return true;
                }

                let mut state = guild_state.lock().unwrap();
                match state.server_refcounts.entry(guild_id) {
                    Entry::Vacant(_) => {
                        unreachable!("Guilds should always have an entry in the refcount table");
                    }
                    Entry::Occupied(x) => {
                        let val = *x.get();
                        if val > 0 {
                            return false;
                        }

                        x.remove();
                        if val < 0 {
                            let uval = -val as usize;
                            state.orphan_refcounts.insert(guild_id, uval);
                        }

                        responded_with_yes = true;
                        return true;
                    }
                }
            };

            loop {
                if let Err(x) =
                    serve_channel(&*http, my_channel_id, &mut pubsub_reader, &mut should_exit)
                {
                    error!("Failed serving guild #{}: {}", guild_id, x);
                }

                if should_exit() {
                    info!("Shut down serving for #{}", guild_id);
                    return;
                }
            }
        });
    }

    fn guild_delete(
        &self,
        _ctx: Context,
        incomplete_guild: PartialGuild,
        _full_data: Option<Arc<RwLock<Guild>>>,
    ) {
        info!("Guild #{} has been deleted", incomplete_guild.id);
        self.decref_guild_server(incomplete_guild.id);
    }

    fn guild_unavailable(&self, _ctx: Context, guild_id: GuildId) {
        warn!("Guild #{} is now unavailable", guild_id);
        self.decref_guild_server(guild_id);
    }
}

/// The UI is basically what should be sent at any given time. Once a UI is published, it's
/// immutable.
#[derive(Clone)]
struct UI {
    // FIXME: This pre-splitting is silly. If there's a split, it should be need-based (e.g.,
    // nearing the 2K codepoint limit), rather than arbitrarily based on hope.
    messages: Vec<String>,
    // FIXME: This is broken if a client drops a message. This should be more of a sticky bit for
    // each client. Maybe integrating pubsub more deeply is gonna be necessary...
    force_ping_after_refresh: bool,
}

#[derive(Debug, Default)]
struct UIUpdater;

fn duration_to_shorthand(dur: chrono::Duration) -> String {
    if dur < chrono::Duration::seconds(60) {
        return "<1 minute".into();
    }
    if dur < chrono::Duration::minutes(60) {
        let m = dur.num_minutes();
        return format!("{} {}", m, if m == 1 { "minute" } else { "minutes" });
    }
    if dur < chrono::Duration::days(1) {
        let h = dur.num_hours();
        return format!("{} {}", h, if h == 1 { "hour" } else { "hours" });
    }
    if dur < chrono::Duration::days(28) {
        return format!("{} days", dur.num_days());
    }
    return format!("{} weeks", dur.num_weeks());
}

type NamedBot<'a> = (&'a str, &'a Bot);

impl UIUpdater {
    fn categorize_bots<'a>(
        snapshot: &'a BotStatusSnapshot,
    ) -> (Vec<(&'a str, Vec<NamedBot<'a>>)>, usize) {
        let mut categories: HashMap<&'a str, HashMap<&'a str, &'a Bot>> = HashMap::new();
        let mut skipped = 0usize;
        for (name, bot) in &snapshot.bots {
            if bot.status.state == BuilderState::Offline {
                skipped += 1;
                continue;
            }

            let category = categories.entry(&bot.category).or_default();
            category.insert(name, bot);
        }

        let mut result: Vec<_> = categories
            .into_iter()
            .map(|x| {
                let mut val: Vec<_> = x.1.into_iter().collect();
                val.sort_by_key(|x| x.0);
                (x.0, val)
            })
            .collect();

        assert!(result.iter().all(|x| !x.1.is_empty()));

        result.sort_by_key(|x| x.0);
        (result, skipped)
    }

    fn draw_main_message_from_categories(
        &mut self,
        categories: &[(&str, Vec<(&str, &Bot)>)],
    ) -> String {
        assert!(!categories.is_empty());

        let mut result = String::new();

        let newest_update_time: chrono::NaiveDateTime = categories
            .iter()
            .map(|(_, bots)| {
                bots.iter()
                    .map(|(_, bot)| bot.status.most_recent_build.completion_time)
                    .max()
                    .unwrap()
            })
            .max()
            .unwrap();

        write!(
            result,
            "**Bot summary** (most recent build seen {} ago)",
            duration_to_shorthand(chrono::Utc::now().naive_utc() - newest_update_time),
        )
        .unwrap();

        let all_green: Vec<_> = categories
            .iter()
            .filter_map(|(name, bots)| {
                if bots
                    .iter()
                    .any(|x| x.1.status.first_failing_build.is_some())
                {
                    None
                } else {
                    Some(name)
                }
            })
            .collect();

        if !all_green.is_empty() {
            result += "\n\n:white_check_mark: All builders are OK for ";
            for (i, bot) in all_green.iter().enumerate() {
                if i != 0 {
                    result += ", ";
                    if i + 1 == all_green.len() {
                        result += "and ";
                    }
                }
                result.push('`');
                result += bot;
                result.push('`');
            }
        }

        if all_green.len() == categories.len() {
            result += "\n\n...And no other online bots are broken! WOOHOO! :confetti_ball:";
            return result;
        }

        let all_green: HashSet<_> = all_green.into_iter().collect();
        for (name, bots) in categories {
            if all_green.contains(name) {
                continue;
            }

            let num_red_bots = bots
                .iter()
                .filter(|x| x.1.status.first_failing_build.is_some())
                .count();

            write!(
                result,
                "\n- {}: {} of {} {} broken",
                name,
                num_red_bots,
                bots.len(),
                if bots.len() == 1 {
                    "bot is"
                } else {
                    "bots are"
                },
            )
            .unwrap();

            if num_red_bots == bots.len() {
                result += " :fire:";
            }
        }

        result
    }

    // FIXME: Merging exception/failure is probably bad?
    fn draw_ui_with_snapshot(&mut self, snapshot: &BotStatusSnapshot) -> UI {
        let (categorized, num_offline) = Self::categorize_bots(snapshot);
        if categorized.is_empty() {
            return UI {
                messages: vec![format!("All {} known bots appear offline", num_offline)],
                force_ping_after_refresh: false,
            };
        }

        let mut messages = Vec::new();

        messages.push({
            let mut main_message = self.draw_main_message_from_categories(&categorized);
            if num_offline > 0 {
                write!(
                    main_message,
                    "\n\n({} {} omitted, since {} offline)",
                    num_offline,
                    if num_offline == 1 { "bot" } else { "bots" },
                    if num_offline == 1 { "it's" } else { "they're" },
                )
                .unwrap()
            }
            main_message
        });

        let start_time = chrono::Utc::now().naive_utc();
        for (category_name, bots) in categorized {
            let mut this_message = String::new();
            for (bot_name, bot) in bots {
                let first_failed = match &bot.status.first_failing_build {
                    None => continue,
                    Some(x) => x,
                };

                // Lazily write the header, since we filter out empty messages below.
                if this_message.is_empty() {
                    write!(this_message, "**Broken for `{}`**:", category_name).unwrap();
                }

                let time_broken: String = if start_time < first_failed.completion_time {
                    warn!(
                        "Apparently {:?} failed in the future (current time = {})",
                        first_failed, start_time
                    );
                    "?m".into()
                } else {
                    duration_to_shorthand(start_time - first_failed.completion_time)
                };

                write!(
                    this_message,
                    "\n- For {}: http://lab.llvm.org:8011/builders/{}",
                    time_broken, bot_name,
                )
                .unwrap();
            }

            if !this_message.is_empty() {
                messages.push(this_message);
            }
        }

        UI {
            messages,
            force_ping_after_refresh: false,
        }
    }
}

async fn draw_ui(
    mut events: watch::Receiver<Option<Arc<BotStatusSnapshot>>>,
    pubsub: Arc<Pubsub<Arc<UI>>>,
) {
    let mut looped_before = false;
    let mut updater = UIUpdater::default();
    loop {
        let snapshot = match events.recv().await {
            Some(Some(x)) => x,
            Some(None) => {
                if looped_before {
                    error!("UI drawer got an unexpected `None` BB info");
                }
                continue;
            }
            None => {
                info!("UI events channel is gone; drawer is shutting down");
                return;
            }
        };

        looped_before = true;
        pubsub.publish(Arc::new(updater.draw_ui_with_snapshot(&*snapshot)));
    }
}

pub(crate) fn run(
    token: &str,
    snapshots: watch::Receiver<Option<Arc<BotStatusSnapshot>>>,
    executor: TaskExecutor,
) -> FailureOr<()> {
    let pubsub = Pubsub::new();
    executor.spawn(draw_ui(snapshots, pubsub.clone()));
    let handler = MessageHandler {
        pubsub: pubsub,
        servers: Default::default(),
    };
    let mut client = serenity::Client::new(token, handler)?;
    client.start()?;
    Ok(())
}
