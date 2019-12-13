#!/usr/bin/env python3
"""Syncs, builds, and runs the bot."""

import logging
import itertools
import os
import shutil
import subprocess
import sys
import threading
import time


class Runner:
    def __init__(self, exe_loc, logs_dir, discord_token):
        self._discord_token = discord_token
        self._logs_dir = logs_dir
        self._exe_loc = exe_loc
        self._proc = None
        self._lock = threading.Lock()
        self._cond = threading.Condition(lock=self._lock)
        self._binary_version = 0

    def run_forever(self):
        try:
            self._run()
        except Exception:
            logging.exception('Failed to run the monitor')
            sys.exit(1)

    def _run(self):
        initial_binary_version = 0
        with self._lock:
            while 1:
                while self._binary_version == initial_binary_version:
                    self._cond.wait()

                initial_binary_version = self._binary_version
                if self._proc:
                    if self._proc.poll() is None:
                        self._proc.kill()
                        self._proc.wait()
                    self._proc = None

                log_file = os.path.join(self._logs_dir,
                                        str(int(time.time())) + '.log')
                logging.info('Spawning monitor (logs available at %s)',
                             log_file)
                with open(log_file, 'w') as f:
                    self._proc = subprocess.Popen(
                        [
                            self._exe_loc,
                            '--discord_token=' + self._discord_token,
                        ],
                        stdin=subprocess.DEVNULL,
                        stdout=f,
                        stderr=f,
                    )

    def push_new_binary(self, binary_loc):
        with self._lock:
            temp_loc = self._exe_loc + '.new'
            shutil.copyfile(binary_loc, temp_loc)
            shutil.copymode(binary_loc, temp_loc)
            os.rename(temp_loc, self._exe_loc)
            self._binary_version += 1
            self._cond.notify_all()


def rebuild_and_test(base_dir):
    subprocess.check_call(
        [
            'cargo',
            'test',
            '--release',
        ],
        cwd=base_dir,
    )

    subprocess.check_call(
        [
            'cargo',
            'build',
            '--release',
        ],
        cwd=base_dir,
    )
    return os.path.join(base_dir, 'target', 'release', 'llvm_buildbot_monitor')


def try_fetch_and_rebuild(git_dir, last_head):
    try:
        subprocess.check_call(['git', 'fetch', '-q'], cwd=git_dir)
    except subprocess.CalledProcessError:
        logging.error('Fetching git repo failed')
        return last_head, None

    # If this fails, let everything crash & burn
    origin_master_sha = subprocess.check_output(
        ['git', 'rev-parse', 'origin/master'],
        cwd=git_dir,
    ).strip()
    origin_master_sha = origin_master_sha.decode('utf-8')

    if last_head == origin_master_sha:
        return last_head, None

    if last_head:
        logging.info('New SHA %s found; attempting redeploy...',
                     origin_master_sha)

    subprocess.check_call(['git', 'checkout', origin_master_sha], cwd=git_dir)
    last_head = origin_master_sha

    try:
        new_binary = rebuild_and_test(git_dir)
    except Exception:
        logging.exception('Pre-deploy tests failed unexpectedly')
        new_binary = None

    return origin_master_sha, new_binary


def monitor_git(git_dir, on_new_binary):
    last_head = None
    for check_number in itertools.count(start=1):
        if check_number % 10 == 0:
            logging.info('Check #%d for updates', check_number)
        last_head, new_binary = try_fetch_and_rebuild(git_dir, last_head)
        if new_binary:
            on_new_binary(new_binary)
        time.sleep(5 * 60)


# (Despite this living in the same git repository, having this script reload
# itself seems potentially sketchy? Eh.)
def main():
    logging.basicConfig(
        level=logging.INFO,
        format=
        '[%(asctime)s] %(filename)s:%(lineno)d %(levelname)s: %(message)s',
    )

    my_dir = os.path.join(os.getenv('HOME'), 'llvmbb_monitor')

    discord_token = os.getenv('DISCORD_TOKEN')
    if not discord_token:
      sys.exit('Set DISCORD_TOKEN=something')

    exe_loc = os.path.join(my_dir, 'run_bot')
    git_repo_dir = os.path.join(my_dir, 'git')
    logs_dir = os.path.join(my_dir, 'logs')

    for d in [my_dir, logs_dir]:
        os.makedirs(d, mode=0o755, exist_ok=True)

    if not os.path.exists(git_repo_dir):
        subprocess.check_call([
            'git',
            'clone',
            'https://github.com/gburgessiv/llvmbb-monitor',
            git_repo_dir,
        ])

    runner = Runner(exe_loc, logs_dir, discord_token)
    runner_thread = threading.Thread(target=runner.run_forever)
    runner_thread.daemon = True
    runner_thread.start()

    monitor_git(git_repo_dir, runner.push_new_binary)


if __name__ == '__main__':
    main()
