#!/usr/bin/env python3
"""Syncs, builds, and runs the bot."""

import logging
import os
import shutil
import subprocess
import threading
import time


class Runner:
    def __init__(self, exe_loc):
        self._exe_loc = exe_loc
        self._proc = None
        self._lock = threading.Lock()
        self._cond = threading.Condition(lock=self._lock)
        self._binary_version = 0

    def run(self):
        initial_binary_version = 0
        with self._lock:
            while 1:
                while self._binary_version == initial_binary_version:
                    self._cond.wait()

                initial_binary_version = self._binary_version
                if self._proc:
                    try:
                        self._proc.wait(timeout=0)
                    except TimeoutError:
                        self._proc.kill()
                        self._proc.wait()
                    self._proc = None

                self._proc = subprocess.Popen([self._exe_loc],
                                              stdin=subprocess.DEVNULL)

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


def monitor_git(git_dir, on_new_binary):
    last_head = None
    master = 'origin/master'
    while 1:
        origin_master_sha = subprocess.check_output(
            ['git', 'rev-parse', master]).strip()
        if origin_master_sha != last_head:
            if last_head is not None:
                logging.info('New SHA %s found; attempting redeploy...',
                             origin_master_sha)
            subprocess.check_call(
                [
                    'git',
                    'checkout',
                    master,
                ],
                cwd=git_dir,
            )
            last_head = origin_master_sha
            try:
                new_binary = rebuild_and_test(git_dir)
            except Exception:
                logging.exception('Pre-deploy tests failed unexpectedly')
            else:
                on_new_binary(new_binary)

        time.sleep(5 * 60)


# (Despite this living in the same git repository, having this script reload
# itself seems potentially sketchy? Eh.)
def main():
    my_dir = os.path.join(os.getenv('HOME'), 'llvmbb_monitor')
    exe_loc = os.path.join(my_dir, 'run_bot')
    git_repo_dir = os.path.join(my_dir, 'git')

    os.makedirs(my_dir, mode=0o755, exist_ok=True)

    if not os.path.exists(git_repo_dir):
        subprocess.check_call([
            'git',
            'clone',
            'https://github.com/gburgessiv/llvmbb-monitor',
            git_repo_dir,
        ])
    else:
        subprocess.check_call(
            [
                'git',
                'fetch',
            ],
            cwd=git_repo_dir,
        )

    runner = Runner(exe_loc)
    runner_thread = threading.Thread(target=runner.run)
    runner_thread.daemon = True
    runner_thread.start()

    monitor_git(git_repo_dir, runner.push_new_binary)


if __name__ == '__main__':
    main()
