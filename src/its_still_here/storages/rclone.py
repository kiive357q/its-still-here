"""Rclone storage provider."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from pathlib import Path

from its_still_here.config import StorageConfig
from its_still_here.storages.base import BaseStorageProvider

logger = logging.getLogger(__name__)


class RcloneProvider(BaseStorageProvider):
    """Storage provider that delegates uploads/verification to the rclone CLI.

    The provider builds an isolated subprocess environment for every command
    so that proxy variables are **never** written to the global
    :data:`os.environ`.

    Parameters
    ----------
    config:
        Must have ``type == "rclone"`` and a valid ``remote_name``.
    """

    def __init__(self, config: StorageConfig) -> None:
        super().__init__(config)

    # ------------------------------------------------------------------
    # BaseStorageProvider interface
    # ------------------------------------------------------------------

    def upload(self, local_path: Path, remote_path: str) -> bool:
        """Upload *local_path* to the rclone remote.

        Runs::

            rclone copyto <local_path> <remote_name>:<root_path>/<remote_path>
                --drive-chunk-size <chunk_size>
                --retries <retries>
                --low-level-retries 20
                --tpslimit <tpslimit>

        Parameters
        ----------
        local_path:
            Absolute local file path.
        remote_path:
            Destination path relative to ``config.root_path``.

        Returns
        -------
        bool
            ``True`` on success (rclone exit code 0).
        """
        destination = self._remote_dest(remote_path)
        cmd = [
            "rclone",
            "copyto",
            str(local_path),
            destination,
            "--drive-chunk-size", self.config.chunk_size,
            "--retries", str(self.config.retries),
            "--low-level-retries", "20",
            "--tpslimit", str(self.config.tpslimit),
        ]
        logger.info(
            "RcloneProvider [%s] uploading %s → %s",
            self.config.name,
            local_path,
            destination,
        )
        result = subprocess.run(
            cmd,
            env=self._build_env(),
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            logger.error(
                "RcloneProvider [%s] upload failed (exit %d):\n%s",
                self.config.name,
                result.returncode,
                result.stderr,
            )
            return False
        logger.info("RcloneProvider [%s] upload succeeded: %s", self.config.name, destination)
        return True

    def verify(self, local_path: Path, remote_path: str) -> bool:
        """Verify the remote file size matches the local file.

        Runs::

            rclone lsf --format s <remote_name>:<root_path>/<remote_path>

        and compares the reported size with :meth:`Path.stat`.

        Parameters
        ----------
        local_path:
            Local reference file.
        remote_path:
            Remote path relative to ``config.root_path``.

        Returns
        -------
        bool
            ``True`` when sizes match.
        """
        destination = self._remote_dest(remote_path)
        cmd = ["rclone", "lsf", "--format", "s", destination]
        result = subprocess.run(
            cmd,
            env=self._build_env(),
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            logger.error(
                "RcloneProvider [%s] lsf failed (exit %d): %s",
                self.config.name,
                result.returncode,
                result.stderr,
            )
            return False

        raw = result.stdout.strip()
        try:
            remote_size = int(raw)
        except ValueError:
            logger.error(
                "RcloneProvider [%s] unexpected lsf output: %r", self.config.name, raw
            )
            return False

        try:
            local_size = local_path.stat().st_size
        except OSError as exc:
            logger.error("RcloneProvider [%s] cannot stat local file: %s", self.config.name, exc)
            return False

        match = remote_size == local_size
        if not match:
            logger.warning(
                "RcloneProvider [%s] size mismatch: local=%d remote=%d",
                self.config.name,
                local_size,
                remote_size,
            )
        return match

    def check_health(self) -> tuple[bool, str]:
        """Check rclone availability, proxy TCP reachability, and remote access.

        Returns
        -------
        tuple[bool, str]
            ``(True, "OK")`` or ``(False, <reason>)``.
        """
        # 1. rclone binary present?
        if shutil.which("rclone") is None:
            return False, "rclone executable not found in PATH."

        # 2. Proxy reachable?
        proxy_cfg = self.config.proxy
        if proxy_cfg.enabled and proxy_cfg.url:
            ok, msg = self.check_proxy_reachable(proxy_cfg.url)
            if not ok:
                return False, msg

        # 3. Remote accessible?
        remote_name = self.config.remote_name
        if not remote_name:
            return False, "remote_name is not configured."

        cmd = ["rclone", "about", f"{remote_name}:"]
        result = subprocess.run(
            cmd,
            env=self._build_env(),
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            return (
                False,
                f"'rclone about {remote_name}:' failed (exit {result.returncode}): "
                f"{result.stderr.strip()}",
            )
        return True, "OK"

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _remote_dest(self, remote_path: str) -> str:
        """Build the full rclone destination string."""
        root = self.config.root_path.rstrip("/")
        rel = remote_path.lstrip("/")
        return f"{self.config.remote_name}:{root}/{rel}"

    def _build_env(self) -> dict[str, str]:
        """Return a copy of ``os.environ`` with proxy vars injected if needed.

        This method **never** mutates :data:`os.environ`.
        """
        env = dict(os.environ)
        proxy_cfg = self.config.proxy
        if proxy_cfg.enabled and proxy_cfg.url:
            proxy_url = proxy_cfg.url
            env["HTTP_PROXY"] = proxy_url
            env["HTTPS_PROXY"] = proxy_url
            env["ALL_PROXY"] = proxy_url
        return env
