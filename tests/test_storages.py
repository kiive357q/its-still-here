"""Unit tests for RcloneProvider, WebDAVProvider, and WatchFolderAdapter."""

from __future__ import annotations

import os
import subprocess
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from its_still_here.config import ProxyConfig, SourceConfig, StorageConfig
from its_still_here.core.models import FileEvent
from its_still_here.sources.folder import WatchFolderAdapter
from its_still_here.storages.rclone import RcloneProvider
from its_still_here.storages.webdav import WebDAVProvider


# =====================================================================
# RcloneProvider Tests
# =====================================================================


class TestRcloneProvider:
    """Tests for RcloneProvider.upload() and RcloneProvider.verify()."""

    @staticmethod
    def _make_config(
        *,
        proxy_enabled: bool = False,
        proxy_url: str | None = "http://127.0.0.1:7890",
    ) -> StorageConfig:
        return StorageConfig(
            name="gdrive_backup",
            type="rclone",
            remote_name="my_gdrive",
            root_path="ItIsStillHere",
            chunk_size="64M",
            retries=10,
            tpslimit=8,
            proxy=ProxyConfig(enabled=proxy_enabled, url=proxy_url),
        )

    def test_upload_injects_proxy_only_in_subprocess_env_when_enabled(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Proxy env vars are injected into subprocess env when proxy.enabled=True without mutating os.environ."""
        for var in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
            monkeypatch.delenv(var, raising=False)

        local_file = tmp_path / "report.pdf"
        local_file.write_bytes(b"hello rclone")

        provider = RcloneProvider(
            self._make_config(proxy_enabled=True, proxy_url="http://127.0.0.1:7890")
        )
        completed = subprocess.CompletedProcess(
            args=["rclone"], returncode=0, stdout="", stderr=""
        )

        with patch("its_still_here.storages.rclone.subprocess.run", return_value=completed) as mock_run:
            ok = provider.upload(local_file, "chat_a/report.pdf")

        assert ok is True
        mock_run.assert_called_once()
        cmd = mock_run.call_args.args[0]
        assert cmd == [
            "rclone",
            "copyto",
            str(local_file),
            "my_gdrive:ItIsStillHere/chat_a/report.pdf",
            "--drive-chunk-size",
            "64M",
            "--retries",
            "10",
            "--low-level-retries",
            "20",
            "--tpslimit",
            "8",
        ]
        kwargs = mock_run.call_args.kwargs
        assert kwargs["capture_output"] is True
        assert kwargs["text"] is True
        sub_env = kwargs["env"]
        assert sub_env["HTTP_PROXY"] == "http://127.0.0.1:7890"
        assert sub_env["HTTPS_PROXY"] == "http://127.0.0.1:7890"
        assert sub_env["ALL_PROXY"] == "http://127.0.0.1:7890"

        # Global os.environ must never be mutated
        assert "HTTP_PROXY" not in os.environ
        assert "HTTPS_PROXY" not in os.environ
        assert "ALL_PROXY" not in os.environ

    def test_upload_does_not_inject_proxy_when_disabled(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """HTTP_PROXY / HTTPS_PROXY are NOT injected into subprocess env when proxy.enabled=False."""
        for var in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
            monkeypatch.delenv(var, raising=False)

        local_file = tmp_path / "archive.zip"
        local_file.write_bytes(b"zipdata")

        provider = RcloneProvider(
            self._make_config(proxy_enabled=False, proxy_url="http://127.0.0.1:7890")
        )
        completed = subprocess.CompletedProcess(
            args=["rclone"], returncode=0, stdout="", stderr=""
        )

        with patch("its_still_here.storages.rclone.subprocess.run", return_value=completed) as mock_run:
            ok = provider.upload(local_file, "/chat_b/archive.zip")

        assert ok is True
        sub_env = mock_run.call_args.kwargs["env"]
        assert "HTTP_PROXY" not in sub_env
        assert "HTTPS_PROXY" not in sub_env
        assert "ALL_PROXY" not in sub_env
        assert "HTTP_PROXY" not in os.environ
        assert "HTTPS_PROXY" not in os.environ

    def test_upload_returns_false_on_nonzero_exit(self, tmp_path: Path) -> None:
        """upload() returns False when rclone exits with a non-zero returncode."""
        local_file = tmp_path / "fail.bin"
        local_file.write_bytes(b"123")

        provider = RcloneProvider(self._make_config(proxy_enabled=False))
        failed = subprocess.CompletedProcess(
            args=["rclone"], returncode=1, stdout="", stderr="permission denied"
        )

        with patch("its_still_here.storages.rclone.subprocess.run", return_value=failed):
            assert provider.upload(local_file, "chat/fail.bin") is False

    def test_verify_proxy_env_and_size_matching(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """verify() checks remote size via rclone lsf and respects proxy isolation."""
        for var in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
            monkeypatch.delenv(var, raising=False)

        payload = b"0123456789abcdef"
        local_file = tmp_path / "data.bin"
        local_file.write_bytes(payload)

        # Case 1: proxy enabled + matching size
        provider_proxy = RcloneProvider(
            self._make_config(proxy_enabled=True, proxy_url="socks5://127.0.0.1:1080")
        )
        matched = subprocess.CompletedProcess(
            args=["rclone"], returncode=0, stdout=f"{len(payload)}\n", stderr=""
        )
        with patch("its_still_here.storages.rclone.subprocess.run", return_value=matched) as mock_run:
            assert provider_proxy.verify(local_file, "sub/data.bin") is True

        cmd = mock_run.call_args.args[0]
        assert cmd == ["rclone", "lsf", "--format", "s", "my_gdrive:ItIsStillHere/sub/data.bin"]
        sub_env = mock_run.call_args.kwargs["env"]
        assert sub_env["HTTP_PROXY"] == "socks5://127.0.0.1:1080"
        assert sub_env["HTTPS_PROXY"] == "socks5://127.0.0.1:1080"
        assert "HTTP_PROXY" not in os.environ
        assert "HTTPS_PROXY" not in os.environ

        # Case 2: proxy disabled + size mismatch
        provider_no_proxy = RcloneProvider(
            self._make_config(proxy_enabled=False, proxy_url="socks5://127.0.0.1:1080")
        )
        mismatched = subprocess.CompletedProcess(
            args=["rclone"], returncode=0, stdout="999\n", stderr=""
        )
        with patch("its_still_here.storages.rclone.subprocess.run", return_value=mismatched) as mock_run:
            assert provider_no_proxy.verify(local_file, "sub/data.bin") is False

        sub_env_disabled = mock_run.call_args.kwargs["env"]
        assert "HTTP_PROXY" not in sub_env_disabled
        assert "HTTPS_PROXY" not in sub_env_disabled
        assert "HTTP_PROXY" not in os.environ
        assert "HTTPS_PROXY" not in os.environ

    def test_verify_handles_lsf_error_and_bad_output(self, tmp_path: Path) -> None:
        """verify() returns False on rclone error or non-numeric lsf output."""
        local_file = tmp_path / "doc.txt"
        local_file.write_bytes(b"abc")
        provider = RcloneProvider(self._make_config(proxy_enabled=False))

        err_proc = subprocess.CompletedProcess(
            args=["rclone"], returncode=3, stdout="", stderr="not found"
        )
        with patch("its_still_here.storages.rclone.subprocess.run", return_value=err_proc):
            assert provider.verify(local_file, "doc.txt") is False

        bad_proc = subprocess.CompletedProcess(
            args=["rclone"], returncode=0, stdout="invalid-size\n", stderr=""
        )
        with patch("its_still_here.storages.rclone.subprocess.run", return_value=bad_proc):
            assert provider.verify(local_file, "doc.txt") is False


# =====================================================================
# WebDAVProvider Tests
# =====================================================================


class TestWebDAVProvider:
    """Tests for WebDAVProvider with mocked httpx.Client."""

    @staticmethod
    def _make_config(
        *,
        proxy_enabled: bool = False,
        proxy_url: str | None = "http://127.0.0.1:8888",
    ) -> StorageConfig:
        return StorageConfig(
            name="nextcloud_dav",
            type="webdav",
            base_url="https://dav.example.com/remote.php/dav/files/alice",
            username="alice",
            password="secret",
            root_path="ItIsStillHere",
            proxy=ProxyConfig(enabled=proxy_enabled, url=proxy_url),
        )

    def test_upload_mkcol_recursive_and_streaming_put(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """upload() issues recursive MKCOLs and streams file chunks via PUT."""
        for var in ("HTTP_PROXY", "HTTPS_PROXY"):
            monkeypatch.delenv(var, raising=False)

        payload = b"chunk1-" * 200
        local_file = tmp_path / "video.mp4"
        local_file.write_bytes(payload)

        mock_client = MagicMock(spec=httpx.Client)
        mock_client.is_closed = False

        mkcol_urls: list[str] = []

        def fake_request(method: str, url: str, **kwargs):
            assert method == "MKCOL"
            mkcol_urls.append(url)
            resp = MagicMock()
            resp.status_code = 201
            return resp

        captured_put: dict[str, object] = {}

        def fake_put(url: str, *, content, headers):
            # Consume the generator while the file handle is open inside upload()
            streamed_bytes = b"".join(content)
            captured_put["url"] = url
            captured_put["body"] = streamed_bytes
            captured_put["headers"] = headers
            resp = MagicMock()
            resp.is_success = True
            resp.status_code = 201
            return resp

        mock_client.request.side_effect = fake_request
        mock_client.put.side_effect = fake_put

        provider = WebDAVProvider(
            self._make_config(proxy_enabled=True, proxy_url="http://127.0.0.1:8888")
        )

        with patch("its_still_here.storages.webdav.httpx.Client", return_value=mock_client) as client_cls:
            ok = provider.upload(local_file, "ProjectChat/2026-11/video.mp4")

        assert ok is True
        # Proxy and auth are passed to httpx.Client, never into os.environ
        client_kwargs = client_cls.call_args.kwargs
        assert client_kwargs["auth"] == ("alice", "secret")
        assert client_kwargs["proxy"] == "http://127.0.0.1:8888"
        assert "HTTP_PROXY" not in os.environ
        assert "HTTPS_PROXY" not in os.environ

        # Recursive MKCOL created each intermediate directory under root_path
        base_root = "https://dav.example.com/remote.php/dav/files/alice/ItIsStillHere"
        assert mkcol_urls == [
            f"{base_root}/ProjectChat",
            f"{base_root}/ProjectChat/2026-11",
        ]

        # Streaming PUT uploaded exact bytes with Content-Length header
        assert captured_put["url"] == f"{base_root}/ProjectChat/2026-11/video.mp4"
        assert captured_put["body"] == payload
        assert captured_put["headers"] == {
            "Content-Length": str(len(payload)),
            "Content-Type": "application/octet-stream",
        }

    def test_upload_returns_false_on_http_failure_or_error(self, tmp_path: Path) -> None:
        """upload() returns False on non-2xx PUT status or httpx.HTTPError."""
        local_file = tmp_path / "doc.pdf"
        local_file.write_bytes(b"pdf-bytes")

        mock_client = MagicMock(spec=httpx.Client)
        mock_client.is_closed = False
        mock_client.request.return_value = MagicMock(status_code=405)

        fail_resp = MagicMock()
        fail_resp.is_success = False
        fail_resp.status_code = 507
        fail_resp.text = "Insufficient Storage"
        mock_client.put.return_value = fail_resp

        provider = WebDAVProvider(self._make_config(proxy_enabled=False))

        with patch("its_still_here.storages.webdav.httpx.Client", return_value=mock_client):
            assert provider.upload(local_file, "doc.pdf") is False

            mock_client.put.side_effect = httpx.ConnectError("connection refused")
            assert provider.upload(local_file, "doc.pdf") is False

    def test_verify_head_size_matching(self, tmp_path: Path) -> None:
        """verify() uses HEAD Content-Length to validate remote file size."""
        payload = b"webdav-verify-payload"
        local_file = tmp_path / "check.bin"
        local_file.write_bytes(payload)

        mock_client = MagicMock(spec=httpx.Client)
        mock_client.is_closed = False

        provider = WebDAVProvider(self._make_config(proxy_enabled=False))
        expected_url = (
            "https://dav.example.com/remote.php/dav/files/alice/"
            "ItIsStillHere/chat/check.bin"
        )

        with patch("its_still_here.storages.webdav.httpx.Client", return_value=mock_client):
            # Matching Content-Length -> True
            ok_resp = MagicMock()
            ok_resp.is_success = True
            ok_resp.headers = httpx.Headers({"Content-Length": str(len(payload))})
            mock_client.head.return_value = ok_resp
            assert provider.verify(local_file, "chat/check.bin") is True
            mock_client.head.assert_called_with(expected_url)

            # Size mismatch -> False
            mismatch_resp = MagicMock()
            mismatch_resp.is_success = True
            mismatch_resp.headers = httpx.Headers({"Content-Length": "9999"})
            mock_client.head.return_value = mismatch_resp
            assert provider.verify(local_file, "chat/check.bin") is False

            # Missing Content-Length -> False
            no_len_resp = MagicMock()
            no_len_resp.is_success = True
            no_len_resp.headers = httpx.Headers({})
            mock_client.head.return_value = no_len_resp
            assert provider.verify(local_file, "chat/check.bin") is False

            # Non-success HTTP status (e.g. 404) -> False
            not_found_resp = MagicMock()
            not_found_resp.is_success = False
            not_found_resp.status_code = 404
            mock_client.head.return_value = not_found_resp
            assert provider.verify(local_file, "chat/check.bin") is False

            # Network error -> False
            mock_client.head.side_effect = httpx.ReadTimeout("timeout")
            assert provider.verify(local_file, "chat/check.bin") is False


# =====================================================================
# WatchFolderAdapter Tests
# =====================================================================


class TestWatchFolderAdapter:
    """Tests for WatchFolderAdapter file stabilization and FileEvent emission."""

    def test_stabilization_emits_file_event_for_subfolder_and_ignores_temp_files(
        self,
        tmp_path: Path,
    ) -> None:
        """Start adapter, write file in chat subfolder, verify FileEvent fields, and stop."""
        watch_dir = tmp_path / "watch"
        watch_dir.mkdir()
        chat_dir = watch_dir / "ProjectAlpha"
        chat_dir.mkdir()

        config = SourceConfig(
            name="local_folder",
            type="folder",
            watch_dir=watch_dir,
            stabilize_seconds=0.2,
        )
        adapter = WatchFolderAdapter(config)

        received: list[FileEvent] = []
        done = threading.Event()

        def on_event(event: FileEvent) -> None:
            received.append(event)
            done.set()

        adapter.start(on_event)
        try:
            # Give watchdog observer a moment to arm OS directory handles.
            time.sleep(0.15)

            # Temporary download suffixes must be ignored.
            temp_file = chat_dir / "partial.crdownload"
            temp_file.write_bytes(b"incomplete")

            # Write a real file and then append once before stabilization completes
            # to exercise timer reset / stabilization.
            target_file = chat_dir / "design_spec.pdf"
            target_file.write_bytes(b"part1-")
            time.sleep(0.05)
            with target_file.open("ab") as fh:
                fh.write(b"part2-final")

            expected_content = b"part1-part2-final"

            assert done.wait(timeout=6.0), "Timed out waiting for FileEvent callback"
            # Brief pause to confirm temp file does not also emit an event
            time.sleep(0.3)

            assert len(received) == 1
            evt = received[0]
            assert evt.source_name == "local_folder"
            assert evt.chat_name == "ProjectAlpha"
            assert evt.file_name == "design_spec.pdf"
            assert evt.file_size == len(expected_content)
            assert evt.file_path == target_file
        finally:
            adapter.stop()

        assert adapter._observer is None

    def test_root_level_file_uses_default_chat_name(self, tmp_path: Path) -> None:
        """Files placed directly inside watch_dir resolve chat_name to 'default'."""
        watch_dir = tmp_path / "watch_root"
        watch_dir.mkdir()

        config = SourceConfig(
            name="root_source",
            type="folder",
            watch_dir=watch_dir,
            stabilize_seconds=0.2,
        )
        adapter = WatchFolderAdapter(config)

        received: list[FileEvent] = []
        done = threading.Event()

        def on_event(event: FileEvent) -> None:
            received.append(event)
            done.set()

        adapter.start(on_event)
        try:
            time.sleep(0.15)
            root_file = watch_dir / "notes.txt"
            payload = b"hello root watcher"
            root_file.write_bytes(payload)

            assert done.wait(timeout=6.0), "Timed out waiting for root FileEvent callback"
            assert len(received) == 1
            evt = received[0]
            assert evt.file_name == "notes.txt"
            assert evt.file_size == len(payload)
            assert evt.chat_name == "default"
        finally:
            adapter.stop()
