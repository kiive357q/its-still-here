"""Abstract base class for all storage providers."""

from __future__ import annotations

import socket
from abc import ABC, abstractmethod
from pathlib import Path
from urllib.parse import urlparse

from its_still_here.config import StorageConfig


class BaseStorageProvider(ABC):
    """Common interface that every storage provider must implement.

    Parameters
    ----------
    config:
        Validated :class:`~its_still_here.config.StorageConfig` for this
        storage backend.
    """

    def __init__(self, config: StorageConfig) -> None:
        self.config = config

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def upload(self, local_path: Path, remote_path: str) -> bool:
        """Upload *local_path* to *remote_path* on the backend.

        Parameters
        ----------
        local_path:
            Absolute path of the local file to upload.
        remote_path:
            Destination path relative to :attr:`StorageConfig.root_path`.

        Returns
        -------
        bool
            ``True`` on success, ``False`` on failure.
        """

    @abstractmethod
    def verify(self, local_path: Path, remote_path: str) -> bool:
        """Verify that the remote copy matches the local file.

        Implementations typically compare file sizes (and optionally checksums).

        Parameters
        ----------
        local_path:
            Local file used as the reference.
        remote_path:
            Destination path relative to :attr:`StorageConfig.root_path`.

        Returns
        -------
        bool
            ``True`` when the remote file is confirmed to match.
        """

    @abstractmethod
    def check_health(self) -> tuple[bool, str]:
        """Return a ``(healthy, message)`` tuple.

        Returns
        -------
        tuple[bool, str]
            ``(True, "OK")`` when the backend is reachable and ready, or
            ``(False, <reason>)`` otherwise.
        """

    # ------------------------------------------------------------------
    # Shared helper
    # ------------------------------------------------------------------

    @staticmethod
    def check_proxy_reachable(proxy_url: str) -> tuple[bool, str]:
        """Test TCP reachability of a proxy endpoint.

        Uses :func:`socket.create_connection` with a 5-second timeout so
        the check is fast and does not perform any HTTP handshake.

        Parameters
        ----------
        proxy_url:
            Full proxy URL, e.g. ``"http://127.0.0.1:7890"``.

        Returns
        -------
        tuple[bool, str]
            ``(True, "proxy reachable")`` or ``(False, <reason>)``.
        """
        try:
            parsed = urlparse(proxy_url)
            host = parsed.hostname
            port = parsed.port
            if not host or not port:
                return False, f"Cannot parse host/port from proxy URL: {proxy_url!r}"
            with socket.create_connection((host, port), timeout=5):
                pass
            return True, "proxy reachable"
        except OSError as exc:
            return False, f"Proxy '{proxy_url}' is not reachable: {exc}"
