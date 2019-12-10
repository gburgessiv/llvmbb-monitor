use crate::Bot;
use crate::BotStatusSnapshot;
use crate::BuilderState;
use crate::FailureOr;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use log::{error, info, warn};
use serenity::http::raw::Http;
use serenity::model::prelude::*;
use serenity::prelude::*;
use tokio::runtime::TaskExecutor;
use tokio::sync::watch;

struct PubsubData<T> {
    data: Option<T>,
    version: u64,
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
    version: u64,
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

    /// Allows us to reread the current value on our next next() call, if a value has been supplied
    /// to this Pubsub already.
    fn reset(&mut self) {
        self.version = 0;
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

        debug_assert!(current_ui.messages.len() <= existing_messages.len());
        for _ in current_ui.messages.len()..existing_messages.len() {
            let id = existing_messages.pop().unwrap().id;
            channel_id.delete_message(http, id)?;
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
                info!("Setting up serving for guild #{}", guild_id);
                if let Err(x) =
                    serve_channel(&*http, my_channel_id, &mut pubsub_reader, &mut should_exit)
                {
                    error!("Failed serving guild #{}: {}", guild_id, x);
                }

                if should_exit() {
                    break;
                }

                // If we're seeing errors, it's probs best to sit back for a bit.
                std::thread::sleep(Duration::from_secs(30));

                // Double-check this, since there's a chance we saw errors related to an in-flight
                // shutdown notice.
                if should_exit() {
                    break;
                }

                pubsub_reader.reset();
            }

            info!("Shut down serving for #{}", guild_id);
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
    messages: Vec<String>,
    // FIXME: This is broken if a client drops a message. This should be more of a sticky bit for
    // each client. Maybe integrating pubsub more deeply is gonna be necessary...
    force_ping_after_refresh: bool,
}

// It's actually 2K chars, but I'd prefer premature splitting over off-by-a-littles
const DISCORD_MESSAGE_SIZE_LIMIT: usize = 1900;

/// Discord has a character limit for messages. This function splits at that limit.
///
/// This tries to split on a number of things:
/// - single newlines
/// - spaces
/// - character boundaries
///
/// In that order. If the last one fails, we have been forsaken, and that's OK.
///
/// In theory, in general, there are many things wrong with this implementation. (Breaking up of
/// multiline code blocks, lack of caring about grapheme clusters, etc.) In practice, no single
/// line should get anywhere near Discord's size limit.
///
/// Similarly, Discord trims messages ( :( ), so it's generally hoped/expected that logical
/// sections will have bold headers or something to differentiate them.
fn split_message(message: String, size_limit: usize) -> Vec<String> {
    debug_assert!(size_limit != 0);

    // Cheap check for the overwhelmingly common case.
    if message.len() < size_limit {
        return vec![message];
    }

    let mut result = Vec::new();
    let mut remaining_message = message.as_str();
    while !remaining_message.is_empty() {
        let size_threshold = match remaining_message.char_indices().skip(size_limit).next() {
            Some((end_index, _)) => end_index,
            None => {
                // Peephole for if the above message.len() check was too conservative.
                if result.is_empty() {
                    return vec![message];
                }
                result.push(remaining_message.to_owned());
                break;
            }
        };

        let this_chunk: &str = &remaining_message[..size_threshold];

        // FIXME: Ideally, this should go out of its way to differentiate between "\n\n" and a
        // regular "\n". Preferring the latter is better, since Discord trims messages, so "\n\n"
        // will be 'collapsed' into effectively "\n".
        let end_index = this_chunk
            .rfind("\n")
            .or_else(|| this_chunk.rfind(|c: char| c.is_whitespace()))
            .unwrap_or(size_threshold);

        let (this_piece, rest) = remaining_message.split_at(end_index);
        result.push(this_piece.to_owned());
        remaining_message = rest.trim_start();
    }
    result
}

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

// Eh.
fn is_duration_recentish(dur: chrono::Duration) -> bool {
    dur < chrono::Duration::hours(12)
}

type NamedBot<'a> = (&'a str, &'a Bot);

#[derive(Debug, Default)]
struct UIUpdater;

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
        now: chrono::NaiveDateTime,
    ) -> String {
        assert!(!categories.is_empty());

        let is_bot_red = |bot: &Bot| bot.status.first_failing_build.is_some();

        let all_bots = categories
            .iter()
            .flat_map(|(_, bot_vec)| bot_vec)
            .map(|(_, bot)| bot);
        let num_bots = all_bots.clone().count();
        if num_bots == 0 {
            return "No bots online.".to_owned();
        }

        let num_red_bots = all_bots.filter(|&bot| is_bot_red(bot)).count();
        let mut result = String::new();
        result += "**Bot summary:**";
        write!(
            result,
            "\n{} of {} online bots are broken",
            num_red_bots, num_bots
        )
        .unwrap();

        if num_red_bots == 0 {
            result += "! :eyes: :confetti_ball: :confetti_ball: :confetti_ball:";
            return result;
        }

        result.push(':');
        for (name, bots) in categories {
            let red_bots = bots.iter().filter(|(_, bot)| is_bot_red(bot));
            let num_red_bots = red_bots.clone().count();
            if num_red_bots == 0 {
                continue;
            }

            let num_recent_red_bots = red_bots
                .filter(|x| {
                    let broken_at =
                        x.1.status
                            .first_failing_build
                            .as_ref()
                            .unwrap()
                            .completion_time;
                    is_duration_recentish(now - broken_at)
                })
                .count();

            write!(
                result,
                "\n- `{}`: {} of {} {} {} broken ({} recent)",
                name,
                num_red_bots,
                bots.len(),
                if bots.len() == 1 { "bot" } else { "bots" },
                if num_red_bots == 1 { "is" } else { "are" },
                num_recent_red_bots,
            )
            .unwrap();

            if num_red_bots == bots.len() {
                result += " :fire:";
            }
        }

        let all_green: Vec<_> = categories
            .iter()
            .filter_map(|(name, bots)| {
                if bots.iter().any(|(_, bot)| is_bot_red(bot)) {
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

        let start_time = chrono::Utc::now().naive_utc();
        let mut full_message_text =
            self.draw_main_message_from_categories(&categorized, start_time);

        let mut push_section = |s: String| {
            full_message_text += "\n\n";
            full_message_text += &s;
        };

        for (category_name, bots) in categorized {
            let mut failed_bots: Vec<_> = bots
                .iter()
                .filter_map(|(name, bot)| match &bot.status.first_failing_build {
                    None => None,
                    Some(x) => Some((*name, x.completion_time)),
                })
                .collect();

            if failed_bots.is_empty() {
                continue;
            }

            failed_bots.sort_by_key(|&(_, first_failed_time)| first_failed_time);
            failed_bots.reverse();

            let mut this_message = String::new();
            write!(this_message, "**Broken for `{}`**:", category_name).unwrap();
            for (bot_name, first_failed_time) in failed_bots {
                let time_broken: String = if start_time < first_failed_time {
                    warn!(
                        "Apparently {:?} failed in the future (current time = {})",
                        bot_name, start_time
                    );
                    "?m".into()
                } else {
                    duration_to_shorthand(start_time - first_failed_time)
                };

                write!(
                    this_message,
                    "\n- For {}: http://lab.llvm.org:8011/builders/{}",
                    time_broken, bot_name,
                )
                .unwrap();
            }

            push_section(this_message);
        }

        push_section({
            let mut final_section = String::new();

            final_section += "**Meta:**";
            if num_offline > 0 {
                write!(
                    final_section,
                    "\n- {} {} omitted, since {} offline.",
                    num_offline,
                    if num_offline == 1 { "bot" } else { "bots" },
                    if num_offline == 1 { "it's" } else { "they're" },
                )
                .unwrap()
            }

            let newest_update_time: chrono::NaiveDateTime = snapshot
                .bots
                .values()
                .map(|bot| bot.status.most_recent_build.completion_time)
                .max()
                .unwrap();

            write!(
                final_section,
                "\n- Last build was seen {} ago.",
                duration_to_shorthand(start_time - newest_update_time)
            )
            .unwrap();

            final_section
        });

        UI {
            messages: split_message(full_message_text, DISCORD_MESSAGE_SIZE_LIMIT),
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

#[cfg(test)]
mod test {
    fn checked_split_message(message: &str, size: usize) -> Vec<String> {
        let messages = super::split_message(message.to_owned(), size);
        for m in &messages {
            let n = m.chars().count();
            assert!(
                n <= size,
                "{:?} has {} chars, which is above the size threshold of {}",
                m,
                n,
                size
            );
        }
        messages
    }

    #[test]
    fn check_split_message_prefers_simple_boundaries() {
        assert_eq!(checked_split_message("foo bar", 4), &["foo", "bar"]);
        assert_eq!(checked_split_message("foo\nbar", 4), &["foo", "bar"]);
        assert_eq!(checked_split_message("a\nb c", 2), &["a", "b", "c"]);
        assert_eq!(checked_split_message("a\nb c", 4), &["a", "b c"]);
        assert_eq!(checked_split_message("a b\nc", 4), &["a b", "c"]);
        assert_eq!(checked_split_message("a\nb\nc", 4), &["a\nb", "c"]);
    }
}
