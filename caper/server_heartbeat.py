"""Server heartbeat module for sharing server hostname/port with clients."""

from __future__ import annotations

import logging
import socket
import time
from threading import Thread
from typing import TYPE_CHECKING

from autouri import AutoURI

if TYPE_CHECKING:
    import os

logger = logging.getLogger(__name__)


class ServerHeartbeatTimeoutError(Exception):
    """Exception raised when a heartbeat file is expired."""


class ServerHeartbeat:
    """Server heartbeat to share server's hostname/port with clients."""

    DEFAULT_SERVER_HEARTBEAT_FILE = '~/.caper/default_server_heartbeat'
    DEFAULT_HEARTBEAT_TIMEOUT_MS = 120000
    DEFAULT_INTERVAL_UPDATE_HEARTBEAT_SEC = 60.0

    def __init__(
        self,
        heartbeat_file: str | os.PathLike[str] = DEFAULT_SERVER_HEARTBEAT_FILE,
        heartbeat_timeout: int = DEFAULT_HEARTBEAT_TIMEOUT_MS,
        interval_update_heartbeat: float = DEFAULT_INTERVAL_UPDATE_HEARTBEAT_SEC,
    ) -> None:
        """
        Server heartbeat to share store server's hostname/port with clients.

        Args:
            heartbeat_file:
                Server writes hostname/port on this file.
                Client reads hostname/port from this file.
            heartbeat_timeout:
                Expiration period for a heartbeat file (in milliseconds).
                Client will use a heartbeat file only if it is fresh (within timeout).
            interval_update_heartbeat:
                Period for updtaing a heartbeat file (in seconds).
        """
        self._heartbeat_file = str(heartbeat_file)
        self._heartbeat_timeout = heartbeat_timeout
        self._interval_update_heartbeat = interval_update_heartbeat

        self._stop_it = False
        self._thread = None

    def start(self, port: int, hostname: str | None = None) -> Thread:
        """
        Starts a thread that writes hostname/port of a server on a heartbeat file.

        Args:
            port:
                This port will be written to a heartbeat file.
            hostname:
                Optional hostname to be written to heartbeat file.
                socket.gethostname() will be used if not defined.
        """
        self._thread = Thread(target=self._write_to_file, args=(port, hostname))
        self._thread.start()
        return self._thread

    def is_alive(self) -> bool:
        """Check if the heartbeat thread is alive."""
        return self._thread.is_alive() if self._thread else False

    def stop(self) -> None:
        """Stop the heartbeat thread."""
        self._stop_it = True

        if self._thread:
            self._thread.join()

    def read(self, *, raise_timeout: bool = False) -> tuple[str, int] | None:
        """
        Read from heartbeat file.

        If a heartbeat file is not fresh (mtime difference < timeout) then None is returned.

        Returns:
            Tuple of (hostname, port)
            None if a heartbeat file is not fresh (mtime difference < timeout)
        """
        try:
            u = AutoURI(self._heartbeat_file)
            if not u.exists:
                return None
            time_diff_ms = (time.time() - u.mtime) * 1000.0
            content = u.read().strip('\n')
            hostname, port = content.split(':')

        except (OSError, ValueError, AttributeError):
            logger.exception('Failed to read from a heartbeat file. %s', self._heartbeat_file)
            return None

        # Check if heartbeat file has expired (after successful read)
        is_expired = time_diff_ms > self._heartbeat_timeout
        if is_expired:
            logger.error(
                'Found a heartbeat file but it has been expired (> timeout). %s',
                self._heartbeat_file,
            )
            if raise_timeout:
                msg = (
                    f'Found a heartbeat file but it has expired timeout '
                    f'{self._heartbeat_file} ms > {self._heartbeat_timeout} ms'
                )
                raise ServerHeartbeatTimeoutError(msg)
            return None

        logger.info('Reading hostname/port from a heartbeat file. %s:%s', hostname, port)
        return hostname, int(port)

    def _write_to_file(self, port: int, hostname: str | None = None) -> None:
        """Write hostname/port to a heartbeat file."""
        if not hostname:
            hostname = socket.gethostname()

        logger.info('Server heartbeat thread started.')

        while True:
            try:
                logger.debug('Writing heartbeat: %s, %s', hostname, port)
                AutoURI(self._heartbeat_file).write(f'{hostname}:{port}')
            except OSError:
                logger.exception(
                    'Failed to write to a heartbeat_file. %s', self._heartbeat_file
                )
            cnt = 0
            while cnt < self._interval_update_heartbeat:
                cnt += 1
                if self._stop_it:
                    break
                time.sleep(1)
            if self._stop_it:
                break

        logger.info('Server heartbeat thread ended.')
