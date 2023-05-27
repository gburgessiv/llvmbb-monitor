use crate::storage::Storage;
use crate::{Bot, BotID, BotStatusSnapshot, CompletedBuild, Email};

use std::borrow::{Borrow, Cow};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use log::{error, info, warn};
use serenity::async_trait;
use serenity::client::bridge::gateway::event::ShardStageUpdateEvent;
use serenity::http::Http;
use serenity::model::prelude::*;
use serenity::prelude::*;
use tokio::runtime::Runtime;
use tokio::sync::watch;

// TODO:
// ## Include broken stage names in #buildbot-updates (?)
//   ...Could be useful for visually grouping things. Maybe.

// A bit awkward to hold on purpose, so this isn't Clone/Copy/etc and unregistered multiple times
// Auto-unregistering might be nice, but would require an Arc<InfiniteVec<_>>, or other lifetime
// requirements on the InfiniteVec, and I'm not sure how annoying that'd be to deal with.
#[derive(Debug, Hash, Eq, PartialEq)]
struct InfiniteVecToken {
    value: usize,
}

#[derive(Debug)]
struct InfiniteVec<T>
where
    T: Clone,
{
    values: Vec<T>,
    base_offset: usize,
    offsets: HashMap<InfiniteVecToken, usize>,
    next_token_value: usize,
}

impl<T> Default for InfiniteVec<T>
where
    T: Clone,
{
    fn default() -> Self {
        Self {
            values: Vec::new(),
            base_offset: 0,
            offsets: HashMap::new(),
            next_token_value: 0,
        }
    }
}

impl<T> InfiniteVec<T>
where
    T: Clone,
{
    fn extend<It: IntoIterator<Item = T>>(&mut self, elems: It) {
        self.values.extend(elems);
    }

    fn compact(&mut self) {
        if self.values.is_empty() {
            return;
        }

        let min_offset = match self.offsets.values().min() {
            None => {
                self.values.clear();
                return;
            }
            Some(x) => x,
        };

        let min_index = min_offset - self.base_offset;
        if min_index == 0 {
            return;
        }

        let mut i = 0usize;
        self.values.retain(|_| {
            let keep = i >= min_index;
            i += 1;
            keep
        });
        self.base_offset += min_index;
    }

    fn get_all(&mut self, token: &InfiniteVecToken) -> Vec<T> {
        let end_index = self.values.len() + self.base_offset;
        let start_offset = std::mem::replace(
            self.offsets.get_mut(token).expect("Unknown token"),
            end_index,
        );
        let start_index = start_offset - self.base_offset;
        self.values[start_index..].to_vec()
    }

    fn register(&mut self) -> InfiniteVecToken {
        let value = self.next_token_value;
        self.next_token_value += 1;
        self.offsets.insert(
            InfiniteVecToken { value },
            self.values.len() + self.base_offset,
        );
        InfiniteVecToken { value }
    }

    fn unregister(&mut self, tok: &InfiniteVecToken) {
        self.offsets
            .remove(tok)
            .expect("Removing non-existing token");
    }
}

#[derive(Default)]
struct UIBroadcasterState {
    last_ui: Option<Arc<UI>>,
    failed_bots: InfiniteVec<Arc<BotBuild>>,
}

#[derive(Default)]
struct UIBroadcaster {
    state: Mutex<UIBroadcasterState>,
    updates: Arc<tokio::sync::Notify>,
}

impl UIBroadcaster {
    fn publish(&self, next_ui: Arc<UI>, new_failed_builds: &[Arc<BotBuild>]) {
        let mut state = self.state.lock().unwrap();
        state.last_ui = Some(next_ui);
        state.failed_bots.compact();
        state.failed_bots.extend(new_failed_builds.iter().cloned());
        self.updates.notify_waiters();
    }

    fn receiver(me: &Arc<Self>) -> UIBroadcastReceiver {
        let token = me.state.lock().unwrap().failed_bots.register();
        UIBroadcastReceiver {
            broadcaster: me.clone(),
            token,
            waiter: me.updates.clone(),
        }
    }
}

struct UIBroadcastReceiver {
    broadcaster: Arc<UIBroadcaster>,
    token: InfiniteVecToken,
    waiter: Arc<tokio::sync::Notify>,
}

impl UIBroadcastReceiver {
    async fn next(&mut self) -> (Arc<UI>, Vec<Arc<BotBuild>>) {
        self.waiter.notified().await;
        let mut state = self.broadcaster.state.lock().unwrap();
        let ui = state
            .last_ui
            .as_ref()
            .expect("UI should always have a value after version==0")
            .clone();

        let new_failed_builds = state.failed_bots.get_all(&self.token);
        (ui, new_failed_builds)
    }
}

impl Drop for UIBroadcastReceiver {
    fn drop(&mut self) {
        self.broadcaster
            .state
            .lock()
            .unwrap()
            .failed_bots
            .unregister(&self.token)
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

struct ChannelServer {
    bot_user_id: UserId,
    guild: GuildId,
    status_channel: ChannelId,
    updates_channel: ChannelId,

    ui: Option<Arc<UI>>,
    unsent_breakages: VecDeque<Arc<BotBuild>>,
    storage: Arc<Mutex<Storage>>,
}

struct ServerUIMessage {
    last_value: String,
    id: MessageId,
}

#[derive(Default)]
struct BlamelistCache {
    email_id_mappings: HashMap<Email, Vec<UserId>>,
    // Cache of User ID -> Optional nickname
    known_guild_members: Option<HashMap<UserId, Option<String>>>,
}

impl BlamelistCache {
    async fn append_blamelist(
        &mut self,
        target: &mut String,
        http: &Http,
        blamelist: &[Email],
        guild: GuildId,
        storage: &Mutex<Storage>,
    ) -> Result<()> {
        if blamelist.is_empty() {
            return Ok(());
        }

        if self.known_guild_members.is_none() {
            let mut r: HashMap<UserId, Option<String>> = HashMap::new();
            let mut members_iter = guild.members_iter(http).boxed();
            while let Some(member) = members_iter.next().await {
                let member = member?;
                r.insert(member.user.id, member.nick.clone());
            }
            info!("Fetched {} members from #{}", r.len(), guild);
            self.known_guild_members = Some(r);
        }

        let guild_members = self.known_guild_members.as_ref().unwrap();
        {
            let storage = storage.lock().unwrap();
            for email in blamelist {
                match self.email_id_mappings.entry(email.clone()) {
                    Entry::Occupied(..) => {
                        // Nothing to do, unless `storage` was updated after a prior
                        // iteration of this loop. If that happens, eh. It's a race anyway.
                    }
                    Entry::Vacant(x) => {
                        x.insert(storage.find_userids_for(email)?);
                    }
                }
            }
        }

        // Invariant: All emails in the blamelist have an entry in `email_id_mappings`.

        // Sometimes eliding email addresses doesn't feel like a great idea in the face of
        // adversarial input (e.g., the owner of foo@bar.com can grab foo@baz.com so it
        // never looks like foo@baz.com is on a blamelist, which removes some clarity and
        // is slightly icky).
        //
        // On the other hand, people will hopefully be nice in practice, and it makes the
        // UI cleaner, so...
        let mut emails: Vec<&Email> = Vec::new();
        let mut user_ids: HashSet<UserId> = HashSet::new();
        for email in blamelist {
            let users_to_ping = self.email_id_mappings.get(email).unwrap();
            let mut pinged_anyone = false;
            for u in users_to_ping {
                if guild_members.contains_key(u) {
                    pinged_anyone = true;
                    user_ids.insert(*u);
                }
            }

            if !pinged_anyone {
                emails.push(email);
            }
        }

        emails.sort();

        // Invariant: All users in user_ids have an entry in `guild_members`.
        let mut user_ids: Vec<_> = user_ids.into_iter().collect();
        user_ids.sort_by_key(|x| (guild_members.get(x).unwrap(), *x));

        let to_blame = user_ids
            .into_iter()
            .map(|x| format!("<@{}>", x))
            .chain(emails.into_iter().map(discord_safe_email));

        *target += " (blamelist: ";
        for (i, p) in to_blame.enumerate() {
            if i != 0 {
                *target += ", ";
            }
            *target += &p;
        }
        target.push(')');
        Ok(())
    }
}

fn url_escape_bot_name(bot_name: &str) -> Cow<'_, str> {
    // This is the only case I have to care about at the moment.
    if !bot_name.contains(' ') {
        Cow::Borrowed(bot_name)
    } else {
        Cow::Owned(bot_name.replace(' ', "%20"))
    }
}

// Hack: keep a minimum of `MESSAGE_CACHE_SIZE` around for splitting. Otherwise, we might end up
// writing a new message a while after we start. This is problematic, because it'll show up with
// the bot name/etc, and won't look sufficiently similar to a newline. It may also scroll the
// screen for people who've already scrolled to the top, etc.
//
// This number was chosen arbitrarily.
const MESSAGE_CACHE_SIZE: usize = 5;

impl ChannelServer {
    async fn await_next_ui(&mut self, receiver: &mut UIBroadcastReceiver) -> Arc<UI> {
        let (ui, new_breakages) = receiver.next().await;
        self.ui = Some(ui.clone());
        self.unsent_breakages.extend(new_breakages.into_iter());
        ui
    }

    async fn update_status_channel<S: Borrow<str>>(
        &self,
        http: &Http,
        messages: &[S],
        existing_messages: &mut Vec<ServerUIMessage>,
    ) -> Result<()> {
        let empty_message = "_ _";
        let padding_messages = MESSAGE_CACHE_SIZE.saturating_sub(messages.len());
        let new_messages = messages
            .iter()
            .map(|x| x.borrow())
            .chain(std::iter::repeat(empty_message).take(padding_messages));

        let mut num_messages = 0;
        for (i, data) in new_messages.enumerate() {
            num_messages += 1;
            if let Some(prev_data) = existing_messages.get_mut(i) {
                if *data == prev_data.last_value {
                    continue;
                }

                self.status_channel
                    .edit_message(http, prev_data.id, |m| m.content(data))
                    .await?;

                prev_data.last_value = data.to_owned();
                continue;
            }

            let discord_message = self
                .status_channel
                .send_message(http, |m| m.content(data))
                .await?;
            existing_messages.push(ServerUIMessage {
                last_value: data.to_owned(),
                id: discord_message.id,
            });
        }

        debug_assert!(num_messages <= existing_messages.len());
        for _ in num_messages..existing_messages.len() {
            let id = existing_messages.pop().unwrap().id;
            self.status_channel.delete_message(http, id).await?;
        }
        Ok(())
    }

    async fn update_updates_channel(&mut self, http: &Http) -> Result<()> {
        let mut blamelist_cache = BlamelistCache::default();
        while let Some(next_breakage) = self.unsent_breakages.front() {
            let mut current_message = String::with_capacity(256);

            let (bot_category, bot_name) = match &next_breakage.bot_id {
                BotID::GreenDragon { name } => ("GreenDragon", name.as_str()),
                BotID::Lab { name, .. } => ("Lab", name.as_str()),
            };

            write!(
                current_message,
                "**New build breakage in {}/{}**: <",
                bot_category, bot_name,
            )
            .unwrap();
            match &next_breakage.bot_id {
                BotID::GreenDragon { name } => write!(
                    current_message,
                    "http://green.lab.llvm.org/green/job/{}/{}/",
                    url_escape_bot_name(name),
                    next_breakage.build.id
                ),
                BotID::Lab { id, .. } => {
                    // As much as I'd prefer to use `name` instead of `id` here, buildbot doesn't
                    // properly render `/#/builders/${bot_name}/builds/${build_id}`, so `${bot_id}`
                    // it is...
                    write!(
                        current_message,
                        "http://lab.llvm.org/buildbot/#/builders/{}/builds/{}",
                        id, next_breakage.build.id
                    )
                }
            }
            .unwrap();

            current_message.push('>');
            if next_breakage.build.blamelist.len() > 25 {
                // Some bots have very slow turnarounds. Spraying `updates` with massive blamelists
                // probably hurts more than it helps.
                write!(
                    current_message,
                    " (blamelist: {} contributors)",
                    next_breakage.build.blamelist.len()
                )
                .unwrap();
            } else {
                blamelist_cache
                    .append_blamelist(
                        &mut current_message,
                        http,
                        &next_breakage.build.blamelist,
                        self.guild,
                        &self.storage,
                    )
                    .await?;
            }

            for msg in split_message(current_message, DISCORD_MESSAGE_SIZE_LIMIT) {
                // ... Yeah, we'll end up with awkwardly resent messages if this fails and
                // split_message gives us more than one split.
                //
                // On the bright side, `split_message` shouldn't do that like 99.99% of the time. I
                // hope. 2K chars is a lot, fam.
                self.updates_channel
                    .send_message(http, |m| m.content(msg))
                    .await?;
            }

            self.unsent_breakages.pop_front();
        }
        Ok(())
    }

    async fn serve<F>(
        &mut self,
        http: &Http,
        ui: &mut UIBroadcastReceiver,
        should_exit: &mut F,
    ) -> Result<()>
    where
        F: FnMut() -> bool,
    {
        info!("Removing existing messages in {}", self.status_channel);

        let mut existing_messages: Vec<ServerUIMessage> = Vec::new();
        let mut most_recent_id = MessageId(0);
        loop {
            let max_messages = 50;
            let messages = self
                .status_channel
                .messages(http, |retriever| {
                    retriever.after(most_recent_id).limit(max_messages)
                })
                .await?;

            if messages.is_empty() {
                break;
            }

            let new_most_recent_id = messages.iter().map(|x| x.id).max().unwrap();
            debug_assert!(
                new_most_recent_id > most_recent_id,
                "{} <= {}?",
                new_most_recent_id,
                most_recent_id
            );
            most_recent_id = new_most_recent_id;

            let mut not_mine: Vec<MessageId> = Vec::new();
            for message in messages {
                if message.author.id != self.bot_user_id {
                    not_mine.push(message.id);
                    continue;
                }

                existing_messages.push(ServerUIMessage {
                    last_value: message.content,
                    id: message.id,
                });
            }

            for message_id in not_mine {
                // "You can only bulk delete messages that are under 14 days old," so no bulk
                // delete API for us.
                self.status_channel.delete_message(http, message_id).await?;
            }
        }

        // Mixing new messages + old messages can cause unsightly separators.
        if existing_messages.len() < MESSAGE_CACHE_SIZE {
            info!(
                "{} existing messages in #{}, which is < {}; starting with a clean slate",
                existing_messages.len(),
                self.status_channel,
                MESSAGE_CACHE_SIZE
            );
            for msg in &existing_messages {
                // "You can only bulk delete messages that are under 14 days old," so no bulk
                // delete API for us.
                self.status_channel.delete_message(http, msg.id).await?;
            }
            existing_messages.clear();
        } else {
            info!(
                "Reusing up to {} existing messages from status channel #{}",
                existing_messages.len(),
                self.status_channel
            );
        }

        // Don't assume any order on the message IDs we received (they're generally new -> old per
        // batch, but we're getting batches in the order oldest -> newest, so ...)
        existing_messages.sort_by_key(|x| x.id);
        let mut current_ui: Arc<UI> = match self.ui.as_ref() {
            None => {
                let messages: &[&str] = &[concat!(
                    ":man_construction_worker: one moment -- set-up is in progress... ",
                    ":woman_construction_worker:"
                )];
                self.update_status_channel(http, messages, &mut existing_messages)
                    .await?;
                self.await_next_ui(ui).await
            }
            Some(x) => x.clone(),
        };

        loop {
            self.update_status_channel(http, &current_ui.messages, &mut existing_messages)
                .await?;
            self.update_updates_channel(http).await?;

            if should_exit() {
                return Ok(());
            }

            current_ui = self.await_next_ui(ui).await;
        }
    }
}

struct MessageHandler {
    ui_broadcaster: Arc<UIBroadcaster>,
    // The readonly distribution here makes an RwMutex more appropriate in theory, but there're
    // going to be two threads polling this ~minutely. So let's prefer simpler code.
    servers: Arc<Mutex<GuildServerState>>,
    bot_version: &'static str,
    storage: Arc<Mutex<Storage>>,
}

fn append_discord_safe_email(targ: &mut String, email: &Email) {
    // zero-width space to avoid `@mentions`:
    // https://www.fileformat.info/info/unicode/char/200b/index.htm
    *targ += email.account_with_plus();
    *targ += "@\u{200B}";
    *targ += email.domain();
}

// Because I tried copy-pasting my email from a llvmbb message, and that went spectacularly poorly.
fn remove_zero_width_spaces(x: &str) -> Cow<'_, str> {
    let space = '\u{200B}';
    if !x.contains(space) {
        return Cow::Borrowed(x);
    }

    let result = x.replace(space, "");
    Cow::Owned(result)
}

fn discord_safe_email(email: &Email) -> String {
    let mut s = String::new();
    append_discord_safe_email(&mut s, email);
    s
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

    fn handle_add_email(&self, from_uid: UserId, email: Option<&str>) -> String {
        let raw_email = match email {
            Some(x) => x,
            None => return "Need an email.".into(),
        };

        let email = match Email::parse(&remove_zero_width_spaces(raw_email)) {
            Some(x) => x,
            None => return format!("Invalid email address: {:?}", raw_email),
        };

        match self
            .storage
            .lock()
            .unwrap()
            .add_user_email_mapping(from_uid, &email)
        {
            Ok(_) => format!("OK! {} has been added as your email.", raw_email),
            Err(x) => {
                error!(
                    "Failed adding email {:?} for #{}: {}",
                    raw_email, from_uid, x
                );
                "Internal error :(".into()
            }
        }
    }

    fn handle_list_emails(&self, from_uid: UserId) -> String {
        let mut emails = match self.storage.lock().unwrap().find_emails_for(from_uid) {
            Ok(x) => x,
            Err(x) => {
                error!("Failed finding emails for #{}: {}", from_uid, x);
                return "Internal error :(".into();
            }
        };

        if emails.is_empty() {
            return "No emails are associated with your account.".into();
        }

        let mut result_str = "Email(s) associated with your account: ".to_string();
        emails.sort();
        for (i, e) in emails.into_iter().enumerate() {
            if i != 0 {
                result_str += ", ";
            }
            append_discord_safe_email(&mut result_str, &e);
        }
        result_str
    }

    fn handle_remove_email(&self, from_uid: UserId, email: Option<&str>) -> String {
        let raw_email = match email {
            Some(x) => x,
            None => return "Need an email.".into(),
        };

        let email = match Email::parse(&remove_zero_width_spaces(raw_email)) {
            Some(x) => x,
            None => return format!("Invalid email address: {:?}", raw_email),
        };

        let removed = match self
            .storage
            .lock()
            .unwrap()
            .remove_userid_mapping(from_uid, &email)
        {
            Ok(removed) => removed,
            Err(x) => {
                error!(
                    "Failed adding email {:?} for #{}: {}",
                    raw_email, from_uid, x
                );
                return "Internal error :(".into();
            }
        };

        if removed {
            format!("OK! {} has been removed from your account.", raw_email)
        } else {
            format!("I didn't have {} on file for you.", raw_email)
        }
    }
}

#[async_trait]
impl serenity::client::EventHandler for MessageHandler {
    async fn guild_create(&self, ctx: Context, guild: Guild, _is_new: bool) {
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

        let find_channel_id = |name: &str| match guild
            .channels
            .iter()
            .filter(|x| x.1.name == name)
            .map(|x| x.0)
            .next()
        {
            Some(x) => {
                info!("Identified #{} as my channel in #{}", x, guild_id);
                Some(*x)
            }
            None => {
                error!("No {} channel in #{}; quit", name, guild_id);
                None
            }
        };

        let status_channel = match find_channel_id("buildbot-status") {
            Some(x) => x,
            None => return,
        };

        let updates_channel = match find_channel_id("buildbot-updates") {
            Some(x) => x,
            None => return,
        };

        let bot_user_id = ctx.cache.current_user_id().await;
        let http = ctx.http;
        let mut pubsub_reader = UIBroadcaster::receiver(&self.ui_broadcaster);
        let guild_state = self.servers.clone();
        let storage = self.storage.clone();
        tokio::spawn(async move {
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
                        true
                    }
                }
            };

            let mut server = ChannelServer {
                bot_user_id,
                status_channel,
                updates_channel,

                guild: guild_id,
                ui: None,
                unsent_breakages: VecDeque::new(),
                storage,
            };
            loop {
                info!("Setting up serving for guild #{}", guild_id);
                if let Err(x) = server
                    .serve(&http, &mut pubsub_reader, &mut should_exit)
                    .await
                {
                    error!("Failed serving guild #{}: {}", guild_id, x);
                }

                if should_exit() {
                    break;
                }

                // If we're seeing errors, it's probs best to sit back for a bit.
                tokio::time::sleep(Duration::from_secs(30)).await;

                // Double-check this, since there's a chance we saw errors related to an in-flight
                // shutdown notice.
                if should_exit() {
                    break;
                }
            }

            info!("Shut down serving for #{}", guild_id);
        });
    }

    async fn guild_delete(
        &self,
        _ctx: Context,
        incomplete_guild: GuildUnavailable,
        _full_data: Option<Guild>,
    ) {
        info!("Guild #{} has been deleted", incomplete_guild.id);
        self.decref_guild_server(incomplete_guild.id);
    }

    async fn guild_unavailable(&self, _ctx: Context, guild_id: GuildId) {
        warn!("Guild #{} is now unavailable", guild_id);
        self.decref_guild_server(guild_id);
    }

    async fn shard_stage_update(&self, ctx: Context, event: ShardStageUpdateEvent) {
        if event.new == serenity::gateway::ConnectionStage::Connected {
            info!("New shard connection established");
            ctx.set_activity(Activity::playing(self.bot_version)).await;
        }
    }

    async fn message(&self, ctx: Context, message: Message) {
        if !message.is_private() || message.author.bot {
            return;
        }

        let content = message.content.trim();
        let mut content_fields = content.split_whitespace();
        let from_uid = message.author.id;
        let response: Option<String> = match content_fields.next() {
            Some("add-email") => Some(self.handle_add_email(from_uid, content_fields.next())),
            Some("list-emails") => Some(self.handle_list_emails(from_uid)),
            Some("rm-email") => Some(self.handle_remove_email(from_uid, content_fields.next())),
            Some(_) | None => None,
        };

        let default_content = concat!(
            "Hi! I'm a bot developed at https://github.com/gburgessiv/llvmbb-monitor. ",
            "Please ping gburgessiv either on Discord or github with questions.\n",
            "\n",
            "I currently support a very smol and humble text interface, mostly ",
            "centered around notifications:\n",
            "```\n",
            "add-email foo@bar.com -- pings you when foo@bar.com is in a \n",
            "                         blamelest for a newly-broken build.\n",
            "list-emails -- lists all emails associated with your account.\n",
            "rm-email foo@bar.com -- disassociates foo@bar.com from your \n",
            "                        Discord account.\n",
            "```\n",
            "Essentially, if you choose to associate an email with your Discord ",
            "account, you'll be `@mentioned` every time we'd otherwise mention the ",
            "email in question. Importantly, this includes buildbot breakages.",
        );

        let response = response.unwrap_or_else(|| default_content.into());
        for msg in split_message(response, DISCORD_MESSAGE_SIZE_LIMIT) {
            if let Err(x) = message
                .author
                .direct_message(&ctx, |m| m.content(msg))
                .await
            {
                error!("Failed to respond to user message: {}", x);
                break;
            }
        }
    }
}

/// The UI is basically what should be sent at any given time. Once a UI is published, it's
/// immutable.
#[derive(Clone, Debug)]
struct UI {
    messages: Vec<String>,
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
    assert_ne!(size_limit, 0);

    // Cheap check for the overwhelmingly common case.
    if message.len() < size_limit {
        return vec![message];
    }

    // FIXME: in order for this to work on more general input (e.g., anything with indents), it has
    // to take those indents into account. A split between:
    //
    // foo
    //   bar
    //
    // will have discord trim the `bar` part.

    let mut result = Vec::new();
    let mut remaining_message = message.as_str();
    while !remaining_message.is_empty() {
        let size_threshold = match remaining_message.char_indices().nth(size_limit) {
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

        let end_index = match this_chunk.rfind('\n') {
            Some(backest_index) => {
                // Go hunting a bit to see if we can find a \n that isn't preceeded by another \n.
                let mut candidate = Some(backest_index);
                while let Some(i) = candidate {
                    if !this_chunk[..i].ends_with('\n') {
                        break;
                    }

                    let first_non_newline = match this_chunk[..i].rfind(|x| x != '\n') {
                        None => break,
                        Some(i) => i,
                    };

                    candidate = this_chunk[..first_non_newline].rfind('\n');
                }

                candidate.unwrap_or(backest_index)
            }
            None => this_chunk
                .rfind(|c: char| c.is_whitespace())
                .unwrap_or(size_threshold),
        };

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
        let d = dur.num_days();
        return format!("{} {}", d, if d == 1 { "day" } else { "days" });
    }
    format!("{} weeks", dur.num_weeks())
}

// Eh.
fn is_duration_recentish(dur: chrono::Duration) -> bool {
    dur < chrono::Duration::hours(12)
}

type NamedBot<'a> = (&'a BotID, &'a Bot);

#[derive(Debug, Default)]
struct StatusUIUpdater;

impl StatusUIUpdater {
    fn categorize_bots<'a>(
        snapshot: &'a BotStatusSnapshot,
    ) -> (Vec<(&'a str, Vec<NamedBot<'a>>)>, usize) {
        let mut categories: HashMap<&'a str, HashMap<&'a BotID, &'a Bot>> = HashMap::new();
        let mut skipped = 0usize;
        for (name, bot) in &snapshot.bots {
            if !bot.status.is_online {
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
        categories: &[(&str, Vec<(&BotID, &Bot)>)],
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
            };
        }

        let start_time = chrono::Utc::now().naive_utc();
        let mut full_message_text =
            self.draw_main_message_from_categories(&categorized, start_time);

        let mut push_section = |s: String| {
            full_message_text += "\n\n";
            full_message_text += &s;
        };

        let mut all_failed_bots: Vec<&BotID> = Vec::new();
        for (category_name, bots) in categorized {
            let mut failed_bots: Vec<_> = bots
                .iter()
                .filter_map(|(name, bot)| bot.status.first_failing_build.as_ref().map(|x| (*name, x.id, x.completion_time)))
                .collect();

            if failed_bots.is_empty() {
                continue;
            }

            all_failed_bots.extend(failed_bots.iter().map(|&(bot_name, _, _)| bot_name));
            failed_bots.sort_by_key(|&(_, _, first_failed_time)| first_failed_time);
            failed_bots.reverse();

            let mut this_message = String::new();
            write!(this_message, "**Broken for `{}`**:", category_name).unwrap();
            for (bot_id, first_failed_id, first_failed_time) in failed_bots {
                let (time_broken, time_broken_str) = if start_time < first_failed_time {
                    warn!(
                        "Apparently {:?} failed in the future (current time = {})",
                        bot_id, start_time
                    );
                    (chrono::Duration::microseconds(0), "?m".into())
                } else {
                    let time_broken = start_time - first_failed_time;
                    (time_broken, duration_to_shorthand(time_broken))
                };

                let emoji: &str = if time_broken < chrono::Duration::hours(1) {
                    " :boom:"
                } else if time_broken < chrono::Duration::hours(6) {
                    " :umbrella:"
                } else {
                    ""
                };

                let (url_prefix, bot_name): (&str, &str) = match &bot_id {
                    BotID::GreenDragon { name } => ("http://green.lab.llvm.org/green/job", name),
                    BotID::Lab { name, .. } => ("http://lab.llvm.org/buildbot/#/builders", name),
                };

                write!(
                    this_message,
                    "\n-{} For {}: <{}/{}> (since #{})",
                    emoji,
                    time_broken_str,
                    url_prefix,
                    url_escape_bot_name(bot_name),
                    first_failed_id,
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
                "\n- Last build was seen at {} UTC.",
                newest_update_time.format("%Y-%m-%d %H:%M:%S")
            )
            .unwrap();

            final_section
        });

        UI {
            messages: split_message(full_message_text, DISCORD_MESSAGE_SIZE_LIMIT),
        }
    }
}

#[derive(Default, Debug)]
struct UpdateUIUpdater {
    previously_broken_bots: Option<HashSet<BotID>>,
}

struct BotBuild {
    bot_id: BotID,
    build: CompletedBuild,
}

impl UpdateUIUpdater {
    fn get_updates(&mut self, snapshot: &BotStatusSnapshot) -> Vec<Arc<BotBuild>> {
        let now_broken: Vec<(&BotID, &CompletedBuild)> = snapshot
            .bots
            .iter()
            .filter_map(|(id, bot)| {
                bot.status.first_failing_build.as_ref().map(|x| (id, x))
            })
            .collect();

        let newly_broken = if let Some(previously_broken) = &self.previously_broken_bots {
            now_broken
                .iter()
                .filter(|(name, _)| !previously_broken.contains(*name))
                .map(|(name, build)| {
                    Arc::new(BotBuild {
                        bot_id: (*name).to_owned(),
                        build: (*build).to_owned(),
                    })
                })
                .collect()
        } else {
            Vec::new()
        };

        self.previously_broken_bots =
            Some(now_broken.into_iter().map(|(id, _)| id.clone()).collect());

        newly_broken
    }
}

// FIXME: Calling is 'the UI' is now kinda outdated, since we have two of them. Maybe rename.
async fn draw_ui(
    mut events: watch::Receiver<Option<Arc<BotStatusSnapshot>>>,
    pubsub: Arc<UIBroadcaster>,
) {
    let mut looped_before = false;
    let mut status_ui = StatusUIUpdater::default();
    let mut update_ui = UpdateUIUpdater::default();

    loop {
        if let Err(_) = events.changed().await {
            info!("UI events channel is gone; drawer is shutting down");
            return;
        }

        let snapshot: Arc<BotStatusSnapshot> = match events.borrow().as_ref() {
            Some(x) => x.clone(),
            None => {
                if looped_before {
                    error!("UI drawer got an unexpected `None` BB info");
                }
                continue;
            }
        };

        looped_before = true;
        pubsub.publish(
            Arc::new(status_ui.draw_ui_with_snapshot(&snapshot)),
            &update_ui.get_updates(&snapshot),
        );
    }
}

pub(crate) fn run(
    token: &str,
    bot_version: &'static str,
    snapshots: watch::Receiver<Option<Arc<BotStatusSnapshot>>>,
    runtime: Runtime,
    storage: Storage,
) -> Result<()> {
    let ui_broadcaster = Arc::new(UIBroadcaster::default());
    let storage = Arc::new(Mutex::new(storage));
    runtime.spawn(draw_ui(snapshots, ui_broadcaster.clone()));
    runtime.block_on(async move {
        serenity::Client::builder(token)
            .event_handler(MessageHandler {
                ui_broadcaster,
                servers: Default::default(),
                bot_version,
                storage,
            })
            .await?
            .start()
            .await?;
        Ok(())
    })
}

#[cfg(test)]
mod test {
    use super::*;

    fn checked_split_message(message: &str, size: usize) -> Vec<String> {
        let messages = split_message(message.to_owned(), size);
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
    fn test_split_message_prefers_simple_boundaries() {
        assert_eq!(checked_split_message("foo bar", 4), &["foo", "bar"]);
        assert_eq!(checked_split_message("foo\nbar", 4), &["foo", "bar"]);
        assert_eq!(checked_split_message("a\nb c", 2), &["a", "b", "c"]);
        assert_eq!(checked_split_message("a\nb c", 4), &["a", "b c"]);
        assert_eq!(checked_split_message("a b\nc", 4), &["a b", "c"]);
        assert_eq!(checked_split_message("a\nb\nc", 4), &["a\nb", "c"]);

        assert_eq!(checked_split_message("a\n\nb\ncd", 6), &["a\n\nb", "cd"]);
        assert_eq!(checked_split_message("a\nb\n\ncd", 6), &["a", "b\n\ncd"]);
    }

    #[test]
    fn test_infinite_vec_appears_to_work() {
        let mut vec = InfiniteVec::<i32>::default();
        let token_a = vec.register();
        assert!(vec.get_all(&token_a).is_empty());

        vec.extend(std::iter::once(1));
        assert_eq!(vec.get_all(&token_a), &[1]);
        assert!(vec.get_all(&token_a).is_empty());

        let token_b = vec.register();
        assert!(vec.get_all(&token_a).is_empty());

        vec.extend(std::iter::once(2));
        assert_eq!(vec.get_all(&token_a), &[2]);
        assert_eq!(vec.get_all(&token_b), &[2]);

        vec.extend(std::iter::once(3));
        let token_c = vec.register();

        vec.extend(std::iter::once(4));
        assert_eq!(vec.get_all(&token_a), &[3, 4]);
        vec.compact();

        assert_eq!(vec.get_all(&token_b), &[3, 4]);
        assert_eq!(vec.get_all(&token_c), &[4]);
        vec.compact();

        assert!(vec.values.is_empty());
    }

    #[test]
    fn test_infinite_vec_compacts_properly_with_no_registration() {
        let mut vec = InfiniteVec::<i32>::default();
        vec.extend(std::iter::once(1));
        vec.compact();
        assert!(vec.values.is_empty());

        let token = vec.register();
        vec.extend(std::iter::once(2));
        vec.unregister(&token);
        vec.compact();
        assert!(vec.values.is_empty());
    }
}
