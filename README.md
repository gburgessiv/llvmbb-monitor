🤖 👀 🤖

## Summary

This is a simple bot that watches LLVM's upstream buildbots @
http://lab.llvm.org/buildbot/#/console , and posts updates to Discord. It can also
announce upcoming events from the LLVM community calendar (e.g. office hours).

In order to receive buildbot notifications, a discord server can have either or
both of the following channels:

- `#buildbot-updates` (receives a new message whenever one or more bots are found to be newly-broken)
- `#buildbot-status` (summarizes the state of all upstream bots in a live dashboard)

The bot assumes that no one posts in `#buildbot-status` but it. If
someone-who-isn't-this-bot _does_ post in `#buildbot-status`, their
messages may be deleted by the bot at some point in the future. The bot does not
delete messages in `#buildbot-updates`.

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

### The way that is both new and shiny

Running `docker build -t llvm_buildbot_monitor .` in this directory will produce
a docker image that gets you the majority of the way there.

The only thing missing is credentials and database info, which are both
specified during `docker run`. My `docker run` command for testing is kept
in the `scripts/run-staging-bot.sh` file.

Notes:
  - The default db file is called `db.sqlite3`. I store it in db/ within
    this repo (though it's gitignore'd).
  - The /logs volume is optional. If it's specified, the default container
    output will be minimal, instead being redirected to log files. If not
    specified, output will go to stdout/stderr.

### The old way

`DISCORD_TOKEN=foo ./run.py` is what I used to run the bot on my server. It's
sorta awkward to hold, since that script is part of this repo, yet it syncs the
bot to somewhere under `~/llvmbb_monitor/`.

...In any case, that script takes care of polling github for new updates, and
automatically building/testing any new commits that fly in. If all of the tests
pass, it'll launch the new version of the bot.
