from __future__ import annotations

import logging
import signal
import time
from subprocess import PIPE, Popen
from threading import Thread
from typing import Any, Callable

logger = logging.getLogger(__name__)
interrupted = False
terminated = False


def sigterm_handler(_signo: int, _frame: Any) -> None:
    global terminated
    logger.info('Received SIGTERM.')
    terminated = True


def sigint_handler(_signo: int, _frame: Any) -> None:
    global interrupted
    logger.info('Received SIGINT.')
    interrupted = True


signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigint_handler)


def is_fileobj_open(fileobj: Any) -> bool:
    return fileobj and not getattr(fileobj, 'closed', False)


class NBSubprocThread(Thread):
    DEFAULT_POLL_INTERVAL_SEC = 0.01
    DEFAULT_SUBPROCESS_NAME = 'Subprocess'
    DEFAULT_STOP_SIGNAL = signal.SIGTERM

    def __init__(
        self,
        args: list[str],
        cwd: str | None = None,
        stdin: Any | None = None,
        on_poll: Callable[[], Any] | None = None,
        on_stdout: Callable[[str], Any] | None = None,
        on_stderr: Callable[[str], Any] | None = None,
        on_finish: Callable[[], Any] | None = None,
        poll_interval: float = DEFAULT_POLL_INTERVAL_SEC,
        quiet: bool = False,
        subprocess_name: str = DEFAULT_SUBPROCESS_NAME,
    ) -> None:
        """Non-blocking STDOUT/STDERR streaming for subprocess.Popen().

        This class makes two daemonized threads for nonblocking
        streaming of STDOUT/STDERR.

        Note that return value of callback functions are updated
        for the following properties:
            - status:
                Updated with return value of on_poll, on_stdout, on_stderr.
                If return value is None then no update.
            - returnvalue:
                Updated with return value of on_finish.
                If return value is None then no update.

        This is useful to check status of the thread and
        get the final return value of the function that this class
        actually runs.

        Args:
            args:
                List of command line arguments.
            cwd:
                subprocess.Popen's cwd.
            stdin:
                subprocess.Popen's stdin.
                Note that subprocess.Popen's stdout/stderr is fixed
                at subprocess.PIPE/subprocess.STDOUT.
            on_poll:
                Callback on every polling.
                If return value is not None then it is used for updating property `status`.
            on_stdout:
                Callback on every non-empty STDOUT line.
                If return value is not None then it is used for updating property `status`.
                This callback function should take one argument:
                    - stdout (str):
                        New incoming STDOUT line string with trailing newline (backslash n).
            on_stderr:
                Callback on every non-empty STDERR line.
                If return value is not None then it is used for updating property `status`.
                This callback function should take one argument:
                    - stderr (str):
                        New incoming STDERR line string with trailing newline (backslash n).
            on_finish:
                Callback on terminating/completing a thread.
                If return value is not None then it is used for updating property `returnvalue`.
            poll_interval (float):
                Polling interval in seconds.
            quiet:
                No logging.
            subprocess_name:
                Subprocess name for logging.
            signal_handler:
                Signal handler for a graceful shutdown.
        """
        super().__init__(
            target=self._popen,
            args=(args, cwd, stdin, on_poll, on_stdout, on_stderr, on_finish),
        )
        self._poll_interval = poll_interval
        self._quiet = quiet
        self._subprocess_name = subprocess_name

        self._stdout_list: list[str] = []
        self._stderr_list: list[str] = []
        self._returncode: int | None = None
        self._stop_it = False
        self._stop_signal: int | None = None
        self._status: Any | None = None
        self._returnvalue: Any | None = None

    @property
    def stdout(self) -> str:
        return ''.join(self._stdout_list)

    @property
    def stderr(self) -> str:
        return ''.join(self._stderr_list)

    def returncode(self) -> int | None:
        """Returns subprocess.Popen.returncode.
        None if not completed or any general Exception occurs.
        """
        return self._returncode

    @property
    def status(self) -> Any | None:
        """Updated with return value of on_poll() for every polling.
        Also updated with return value of on_stdout() or on_stderr()
        """
        return self._status

    @property
    def returnvalue(self) -> Any | None:
        """Updated with return value of on_finish()
        which is called when a thread is terminated.
        None if thread is still running so that on_finish() has not been called yet.
        This works like an actual return value of the function ran inside a thread.
        """
        return self._returnvalue

    def stop(self, stop_signal: int = DEFAULT_STOP_SIGNAL, wait: bool = False) -> None:
        """Subprocess will be teminated after next polling.

        Args:
            wait:
                Wait for a valid returncode (which is not None).
        """
        self._stop_it = True
        self._stop_signal = stop_signal
        if wait:
            if self._returncode is None:
                logger.info('%s: waiting for a graceful shutdown...', self._subprocess_name)
            while True:
                if self._returncode is not None:
                    return
                time.sleep(self._poll_interval)

    def _popen(
        self,
        args: list[str],
        cwd: str | None = None,
        stdin: Any | None = None,
        on_poll: Callable[[], Any] | None = None,
        on_stdout: Callable[[str], Any] | None = None,
        on_stderr: Callable[[str], Any] | None = None,
        on_finish: Callable[[], Any] | None = None,
    ) -> None:
        """Wrapper for subprocess.Popen()."""
        global terminated
        global interrupted

        def read_stdout(stdout_bytes: bytes) -> None:
            text = stdout_bytes.decode()
            if text:
                self._stdout_list.append(text)
                if on_stdout:
                    ret_on_stdout = on_stdout(text)
                    if ret_on_stdout is not None:
                        self._status = ret_on_stdout

        def read_stderr(stderr_bytes: bytes) -> None:
            text = stderr_bytes.decode()
            if text:
                self._stderr_list.append(text)
                if on_stderr:
                    ret_on_stderr = on_stderr(text)
                    if ret_on_stderr is not None:
                        self._status = ret_on_stderr

        def read_from_stdout_obj(stdout: Any) -> None:
            while True:
                if terminated or interrupted:
                    break
                if not is_fileobj_open(stdout):
                    break

        def read_from_stderr_obj(stderr: Any) -> None:
            if is_fileobj_open(stderr):
                for line in iter(stderr.readline, b''):
                    read_stderr(line)

        self._stop_it = False

        try:
            p = Popen(args, stdout=PIPE, stderr=PIPE, cwd=cwd, stdin=stdin)
            thread_stdout = Thread(
                target=read_from_stdout_obj,
                args=(p.stdout,),
                daemon=True,
            )
            thread_stderr = Thread(
                target=read_from_stderr_obj,
                args=(p.stderr,),
                daemon=True,
            )
            thread_stdout.start()
            thread_stderr.start()

            while True:
                if on_poll:
                    ret_on_poll = on_poll()
                    if ret_on_poll is not None:
                        self._status = ret_on_poll
                if p.poll() is not None:
                    self._returncode = p.poll()
                    break

                if terminated or interrupted or (self._stop_it and self._stop_signal):
                    if terminated:
                        current_signal = signal.SIGTERM
                    elif interrupted:
                        current_signal = signal.SIGINT
                    else:
                        current_signal = self._stop_signal or signal.SIGTERM

                    logger.info(
                        'Sending signal %s to subprocess. '
                        'name: %s, pid: %s',
                        current_signal, self._subprocess_name, p.pid,
                    )
                    p.send_signal(current_signal)

                    self._returncode = p.returncode
                    break

                time.sleep(self._poll_interval)

        except Exception:
            if not self._quiet:
                logger.exception("Error in subprocess")
            self._returncode = 127

        else:
            stdout_bytes, stderr_bytes = p.communicate()
            read_stdout(stdout_bytes)
            read_stderr(stderr_bytes)
            self._returncode = p.returncode

        if on_finish:
            ret_on_finish = on_finish()
            if ret_on_finish is not None:
                self._returnvalue = ret_on_finish

        if not self._quiet:
            if self._returncode:
                logger.error(
                    '%s failed. returncode=%s',
                    self._subprocess_name, self._returncode,
                )
            else:
                logger.info(
                    '%s finished successfully.',
                    self._subprocess_name,
                )
