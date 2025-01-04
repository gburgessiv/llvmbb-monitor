#!/bin/bash -eu

if [[ -z "${DISCORD_TOKEN:-}" ]]; then
  echo "Set DISCORD_TOKEN=" >&2
  exit 1
fi

exec_mon() {
  ./llvm_buildbot_monitor/llvm_buildbot_monitor \
    --discord-token="${DISCORD_TOKEN}" \
    --database=/db/db.sqlite3
}

if ! [[ -d "/logs" ]]; then
  echo "Logging output to stdout, due to lack of /logs dir."
  exec_mon
  exit 1
fi

log_file="/logs/$(date "+%F-%T").log"

# If a log is ~6mos old, ... who cares.
find /logs/ -mtime +180 -name '*.log.gz' -delete

shopt -s nullglob
for f in /logs/*.log; do
  echo "Log housekeeping: autocompressing ${f}..."
  gzip -9 "${f}"
done

echo "Logging output to ${log_file}."
exec_mon >& "${log_file}"
