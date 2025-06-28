FROM debian:stable-slim AS build-baseline

RUN apt-get update
ENV LANG=C.UTF-8

RUN apt-get install -y \
  build-essential \
  curl \
  git \
  libssl-dev \
  pkg-config \
  sqlite3

# Set up the base user.
RUN \
  groupadd -g 1000 llvmbb_monitor && \
  useradd -u 1000 -g 1000 llvmbb_monitor -s /bin/bash -d /home/llvmbb_monitor && \
  mkdir /home/llvmbb_monitor && \
  chown llvmbb_monitor:llvmbb_monitor /home/llvmbb_monitor
USER 1000:1000
WORKDIR /home/llvmbb_monitor

# Now set rustup up.
COPY --chmod=555 docker/setup_rustup.sh .
RUN ./setup_rustup.sh
ENV PATH="$PATH:/home/llvmbb_monitor/.cargo/bin"

# Set up a baseline build so any non-dependency-touching changes to src/ and
# such do incremental builds.
# TODO: Is the `cargo vendor` necessary? Feels bad to use it to just populate
# .cargo's cache.
COPY --chown=llvmbb_monitor:llvmbb_monitor Cargo.lock Cargo.toml code/
COPY --chown=llvmbb_monitor:llvmbb_monitor calendar_check/Cargo.toml code/calendar_check/Cargo.toml
COPY --chown=llvmbb_monitor:llvmbb_monitor llvm_buildbot_monitor/Cargo.toml code/llvm_buildbot_monitor/Cargo.toml
RUN \
  mkdir -p code/calendar_check/src && \
  mkdir -p code/llvm_buildbot_monitor/src && \
  cd code && \
  echo 'fn main() {}' > calendar_check/src/main.rs && \
  echo 'fn main() {}' > llvm_buildbot_monitor/src/main.rs && \
  cargo vendor && \
  rm -rf vendor

# Test container
FROM build-baseline AS test-container

# `cargo clean` is needed since otherwise Cargo might not rebuild things
# properly.
RUN \
  cd code && \
  cargo build --offline --locked && \
  cargo clean

# Bring in sources & test go brrr.
COPY --chown=llvmbb_monitor:llvmbb_monitor calendar_check/ code/calendar_check/
COPY --chown=llvmbb_monitor:llvmbb_monitor llvm_buildbot_monitor/ code/llvm_buildbot_monitor/
COPY --chown=llvmbb_monitor:llvmbb_monitor .git/ code/.git
RUN cd code && cargo test

# Release build container.
FROM build-baseline AS build

# `cargo clean` is needed since otherwise Cargo might not rebuild things
# properly.
RUN \
  cd code && \
  cargo build --release --offline --locked && \
  cargo clean -r -p calendar_check && \
  cargo clean -r -p llvm_buildbot_monitor

# Bring in sources & test go brrr.
COPY --chown=llvmbb_monitor:llvmbb_monitor calendar_check/ code/calendar_check/
COPY --chown=llvmbb_monitor:llvmbb_monitor llvm_buildbot_monitor/ code/llvm_buildbot_monitor/
COPY --chown=llvmbb_monitor:llvmbb_monitor .git/ code/.git
RUN cd code && cargo build --release --workspace

# Now build the actual image.
FROM debian:stable-slim

RUN apt-get update
ENV LANG=C.UTF-8

RUN apt-get install -y \
  ca-certificates \
  libssl-dev \
  sqlite3

RUN \
  groupadd -g 1000 llvmbb_monitor && \
  useradd -u 1000 -g 1000 llvmbb_monitor -s /bin/bash -d /home/llvmbb_monitor && \
  mkdir -p /home/llvmbb_monitor/llvm_buildbot_monitor /db && \
  chown -R llvmbb_monitor:llvmbb_monitor /home/llvmbb_monitor /db
USER 1000:1000
WORKDIR /home/llvmbb_monitor

COPY --chmod=555 docker/run_buildbot_monitor.sh .
COPY --from=build \
  /home/llvmbb_monitor/code/target/release/llvm_buildbot_monitor \
  llvm_buildbot_monitor/

CMD ["bash", "-eu", "run_buildbot_monitor.sh"]
