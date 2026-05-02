#!/bin/bash -eux
#
# Script I use to run a staging instance of this bot.

cd "$(dirname "$(readlink -m "$0")")"
cd ..

mkdir -p /tmp/llvmbb_staging_logs
exec docker run \
  --rm \
  --env DISCORD_TOKEN="$(<staging_token.txt)" \
  -v "${PWD}/db":/db \
  -v "/tmp/llvmbb_staging_logs":/logs \
  -it \
  llvm_buildbot_monitor
