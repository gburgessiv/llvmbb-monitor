#!/bin/bash -eu
#
# Script I use to run a staging instance of this bot.

cd "$(dirname "$(readlink -m "$0")")"
cd ..

DISCORD_TOKEN="$(<staging_token.txt)"
export DISCORD_TOKEN

set -x
mkdir -p /tmp/llvmbb_staging_logs
exec docker run \
  --rm \
  --init \
  --env DISCORD_TOKEN \
  -v "${PWD}/db":/db \
  -v "/tmp/llvmbb_staging_logs":/logs \
  -it \
  llvm_buildbot_monitor
