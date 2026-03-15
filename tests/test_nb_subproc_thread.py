import platform
import os
import time

import pytest

from caper.nb_subproc_thread import NBSubprocThread

SH_CONTENTS = """#!/bin/bash

echoerr() { echo "$@" 1>&2; }

NUM=$1
if [ -z "$NUM" ]
then
  NUM=10
fi

echo $NUM

# NUM kitties (1 kitty per sec)
for i in $(seq 1 $NUM)
do
  echo "hello kitty $i-1. (STDOUT)"
  sleep 0.25
  echoerr "hello kitty $i-1. (STDERR)"
  sleep 0.25
  echoerr "hello kitty $i-2. (STDERR)"
  sleep 0.25
  echo "hello kitty $i-2. (STDOUT)"
  sleep 0.25
done

exit 10
"""


def on_stdout(stdout) -> None:
    assert stdout.endswith('\n')


def on_stderr(stderr) -> None:
    assert stderr.endswith('\n')


def on_poll() -> None:
    pass


def on_finish() -> str:
    return 'done'


def test_nb_subproc_thread(tmp_path) -> None:
    sh = tmp_path / 'test.sh'
    sh.write_text(SH_CONTENTS)

    th = NBSubprocThread(
        args=['bash', str(sh)],
        on_poll=on_poll,
        on_stdout=on_stdout,
        on_stderr=on_stderr,
        on_finish=on_finish,
        poll_interval=0.1,
    )
    assert th.returnvalue is None
    assert not th.is_alive()
    th.start()
    assert th.is_alive()
    # rc is None while running
    assert th.returncode is None
    th.join()
    assert th.returncode == 10
    assert th.returnvalue == 'done'


def test_nb_subproc_thread_stopped(tmp_path) -> None:
    sh = tmp_path / 'test.sh'
    sh.write_text(SH_CONTENTS)

    th = NBSubprocThread(args=['bash', str(sh)], on_stdout=on_stdout)
    th.start()
    time.sleep(2)
    assert th.is_alive()
    th.stop(wait=True)
    assert not th.is_alive()
    # rc should be is None if terminated
    assert th.returncode is not None
    # subprocess is terminated until it reaches kitty 4 (4 sec > 2 sec).
    assert 'hello kitty 4' not in th.stderr


def test_nb_subproc_thread_nonzero_rc() -> None:
    for rc in range(10):
        th = NBSubprocThread(args=['bash', '-c', f'exit {rc}'], on_stderr=on_stderr)
        th.start()
        th.join()
        assert th.returncode == rc


# ls on macOS/BSD returns 1 if the file does not exist, Linux returns 2.
# Don't care about windows
LS_RETURN_CODE = 1 if platform.system() == 'Darwin' else 2


@pytest.mark.parametrize(
    ('test_app', 'expected_rc'), [('cat', 1), ('ls', LS_RETURN_CODE), ('java', 1)]
)
def test_nb_subproc_thread_nonzero_rc_for_real_apps(test_app, expected_rc) -> None:
    test_str = 'asdfasf-10190212-zxcv'
    if os.path.exists(test_str):
        msg = 'Test string should not be an existing file.'
        raise ValueError(msg)

    th = NBSubprocThread(args=[test_app, test_str], on_stdout=on_stdout, on_stderr=on_stderr)
    th.start()
    th.join()
    assert th.returncode == expected_rc
    assert test_str in th.stderr
    assert th.stdout == ''
