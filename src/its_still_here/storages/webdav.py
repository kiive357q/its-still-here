"""WebDAV storage provider using httpx."""

from __future__ import annotations

import logging
from pathlib import Path, PurePosixPath
from typing import Any

import httpx

from its_still_here.config import StorageConfig
from its_still_here.storages.base import BaseStorageProvider

logger = logging.getLogger(__name__)

_CHUNK_SIZE = 1 << 20  # 1 MiB streaming chunks


class WebDAVProvider(BaseStorageProvider):
    """Storage provider that uploads files to a WebDAV server via *httpx*.

    Features
    --------
    - Streams uploads in chunks so 200 MB+ files are **never** fully loaded
      into memory.
    - Recursively ``MKCOL``s parent directories before each upload.
    - Verifies via ``HEAD`` request and ``Content-Length`` comparison.
    - Optionally routes all HTTP traffic through a proxy (injected per-client,
      not into ``os.environ``).

    Parameters
    ----------
    config:
        Must have ``type == "webdav"`` and a valid ``base_url``.
    """

    def __init__(self, config: StorageConfig) -> None:
        super().__init__(config)
        self._client: httpx.Client | None = None

    # ------------------------------------------------------------------
    # Context helpers
    # ------------------------------------------------------------------

    def _get_client(self) -> httpx.Client:
        """Return (or lazily create) a reusable :class:`httpx.Client`."""
        if self._client is None or self._client.is_closed:
            self._client = self._build_client()
        return self._client

    def _build_client(self) -> httpx.Client:
        kwargs: dict[str, Any] = {
            "timeout": httpx.Timeout(connect=10.0, read=300.0, write=300.0, pool=10.0),
        }

        # Auth
        username = self.config.username
        password = self.config.password
        if username and password:
            kwargs["auth"] = (username, password)

        # Proxy – injected into the client only, never into os.environ.
        proxy_cfg = self.config.proxy
        if proxy_cfg.enabled and proxy_cfg.url:
            kwargs["proxy"] = proxy_cfg.url

        return httpx.Client(**kwargs)

    # ------------------------------------------------------------------
    # BaseStorageProvider interface
    # ------------------------------------------------------------------

    def upload(self, local_path: Path, remote_path: str) -> bool:
        """Stream *local_path* to the WebDAV server.

        Recursively ``MKCOL``s any missing parent directories first, then
        ``PUT``s the file in streaming mode.

        Parameters
        ----------
        local_path:
            Absolute local file path.
        remote_path:
            Destination path relative to ``config.root_path``.

        Returns
        -------
        bool
            ``True`` on HTTP 2xx response.
        """
        full_remote = self._full_url(remote_path)
        parent_url = self._parent_url(full_remote)

        client = self._get_client()

        # Ensure parent directories exist.
        self._mkcol_recursive(client, parent_url)

        # Stream the upload.
        logger.info(
            "WebDAVProvider [%s] uploading %s → %s",
            self.config.name,
            local_path,
            full_remote,
        )
        try:
            with local_path.open("rb") as fh:
                response = client.put(
                    full_remote,
                    content=_file_chunker(fh),
                    headers={
                        "Content-Length": str(local_path.stat().st_size),
                        "Content-Type": "application/octet-stream",
                    },
                )
            if response.is_success:
                logger.info(
                    "WebDAVProvider [%s] upload succeeded (HTTP %d): %s",
                    self.config.name,
                    response.status_code,
                    full_remote,
                )
                return True
            logger.error(
                "WebDAVProvider [%s] upload failed (HTTP %d): %s",
                self.config.name,
                response.status_code,
                response.text[:500],
            )
            return False
        except httpx.HTTPError as exc:
            logger.exception(
                "WebDAVProvider [%s] HTTP error during upload: %s", self.config.name, exc
            )
            return False

    def verify(self, local_path: Path, remote_path: str) -> bool:
        """Verify via ``HEAD`` that the remote ``Content-Length`` matches local size.

        Parameters
        ----------
        local_path:
            Local reference file.
        remote_path:
            Remote path relative to ``config.root_path``.

        Returns
        -------
        bool
            ``True`` when ``Content-Length`` == ``local_path.stat().st_size``.
        """
        full_remote = self._full_url(remote_path)
        client = self._get_client()
        try:
            response = client.head(full_remote)
        except httpx.HTTPError as exc:
            logger.error(
                "WebDAVProvider [%s] HEAD request failed: %s", self.config.name, exc
            )
            return False

        if not response.is_success:
            logger.error(
                "WebDAVProvider [%s] HEAD returned HTTP %d for %s",
                self.config.name,
                response.status_code,
                full_remote,
            )
            return False

        content_length_raw = response.headers.get("content-length")
        if content_length_raw is None:
            logger.warning(
                "WebDAVProvider [%s] server did not return Content-Length for %s",
                self.config.name,
                full_remote,
            )
            return False

        try:
            remote_size = int(content_length_raw)
        except ValueError:
            logger.error(
                "WebDAVProvider [%s] non-integer Content-Length: %r",
                self.config.name,
                content_length_raw,
            )
            return False

        try:
            local_size = local_path.stat().st_size
        except OSError as exc:
            logger.error(
                "WebDAVProvider [%s] cannot stat local file: %s", self.config.name, exc
            )
            return False

        match = remote_size == local_size
        if not match:
            logger.warning(
                "WebDAVProvider [%s] size mismatch: local=%d remote=%d",
                self.config.name,
                local_size,
                remote_size,
            )
        return match

    def check_health(self) -> tuple[bool, str]:
        """Check proxy reachability (if enabled) and server availability.

        Uses ``PROPFIND`` (depth 0) falling back to ``OPTIONS`` if the server
        does not support WebDAV.

        Returns
        -------
        tuple[bool, str]
            ``(True, "OK")`` or ``(False, <reason>)``.
        """
        proxy_cfg = self.config.proxy
        if proxy_cfg.enabled and proxy_cfg.url:
            ok, msg = self.check_proxy_reachable(proxy_cfg.url)
            if not ok:
                return False, msg

        base_url = self.config.base_url
        if not base_url:
            return False, "base_url is not configured."

        client = self._get_client()

        # Try PROPFIND first (proper WebDAV check).
        try:
            response = client.request(
                "PROPFIND",
                base_url.rstrip("/") + "/",
                headers={"Depth": "0"},
                timeout=10.0,
            )
            # 207 Multi-Status is the canonical success; 405 means the endpoint
            # exists but doesn't support WebDAV PROPFIND (still reachable).
            if response.status_code in {207, 405, 200, 301, 302}:
                return True, "OK"
            # Fall through to OPTIONS check.
        except httpx.HTTPError:
            pass  # Network / TLS error – try OPTIONS.

        try:
            response = client.options(base_url.rstrip("/") + "/", timeout=10.0)
            if response.is_success or response.status_code in {301, 302, 405}:
                return True, "OK"
            return (
                False,
                f"WebDAV server responded with HTTP {response.status_code} to OPTIONS.",
            )
        except httpx.HTTPError as exc:
            return False, f"Cannot reach WebDAV server at {base_url!r}: {exc}"

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _full_url(self, remote_path: str) -> str:
        """Build the absolute WebDAV URL for *remote_path*."""
        base = self.config.base_url or ""
        base = base.rstrip("/")
        root = self.config.root_path.strip("/")
        rel = remote_path.lstrip("/")
        return f"{base}/{root}/{rel}"

    def _parent_url(self, full_url: str) -> str:
        """Return the URL of the parent directory."""
        # Strip the last path segment.
        if "/" not in full_url.lstrip("https://").lstrip("http://"):
            return full_url
        return full_url.rsplit("/", 1)[0]

    def _mkcol_recursive(self, client: httpx.Client, url: str) -> None:
        """Recursively create all ancestor directories via MKCOL.

        Accepts HTTP 201 (created), 405 (already exists), and 301/302
        (redirect / already exists on some servers) as success.
        """
        base = (self.config.base_url or "").rstrip("/")
        root = self.config.root_path.strip("/")
        base_root = f"{base}/{root}"

        # Collect all ancestor segments that need to exist.
        # Strip the scheme+host from the url to get the path portion.
        if url.startswith(base_root):
            rel_portion = url[len(base_root):].strip("/")
        else:
            rel_portion = ""

        if not rel_portion:
            # Nothing to create beyond root; ensure root exists.
            self._mkcol_one(client, base_root)
            return

        parts = PurePosixPath(rel_portion).parts
        current = base_root
        for part in parts:
            current = f"{current}/{part}"
            self._mkcol_one(client, current)

    def _mkcol_one(self, client: httpx.Client, url: str) -> None:
        """Send a single MKCOL request; log but do not raise on failure."""
        try:
            response = client.request("MKCOL", url, timeout=15.0)
            # 201 = created, 405 = already exists, 301/302 = redirect / exists.
            if response.status_code not in {201, 405, 301, 302}:
                logger.debug(
                    "WebDAVProvider [%s] MKCOL %s → HTTP %d",
                    self.config.name,
                    url,
                    response.status_code,
                )
        except httpx.HTTPError as exc:
            logger.debug(
                "WebDAVProvider [%s] MKCOL %s failed: %s", self.config.name, url, exc
            )


def _file_chunker(fh, chunk_size: int = _CHUNK_SIZE):
    """Generator that yields *chunk_size* byte chunks from *fh*."""
    while True:
        data = fh.read(chunk_size)
        if not data:
            break
        yield data
