🤖 👀 🤖

## Summary

This is a simple bot that watches LLVM's upstream buildbots @
http://lab.llvm.org:8011/console , and posts updates to two discord channels.

In order to use this bot, a discord server must exist and have the following two
channels:

- `#buildbot-status`
- `#buildbot-updates`

The bot assumes that no one posts in these channels but it. If
someone-who-isn't-this-bot _does_ post in either of these channels, their
messages may be deleted by the bot at some point in the future.

`#buildbot-status` is a channel that summarizes the state of all upstream bots.
`#buildbot-updates` gets a new message whenever one or more bots are found to be
newly-broken.

If you want this bot in your discord server, feel free to ping me and I'll send
along an invite link.

## Contributing

The bot isn't particularly configurable/general, since it doesn't seem hugely
applicable outside of users of LLVM's [buildbot](http://buildbot.net/) version
(0.8.5).

Contributions are welcome nonetheless. Though this bot isn't officially
sponsored/supported by LLVM in any way (e.g., it's a hobby project), I ask that
you observe LLVM's [code of conduct](https://llvm.org/docs/CodeOfConduct.html)
while you're here. :)

## Running the bot

`DISCORD_TOKEN=foo ./run.py` is what I use to run the bot on my server. It's
sorta awkward to hold, since that script is part of this repo, yet it syncs the
bot to somewhere under `~/llvmbb_monitor/`.

...In any case, that script takes care of polling github for new updates, and
automatically building/testing any new commits that fly in. If all of the tests
pass, it'll launch the new version of the bot.
