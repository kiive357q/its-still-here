"""End-to-end tests for SyncPipeline and CLI commands."""

from __future__ import annotations

import hashlib
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from typer.testing import CliRunner

import its_still_here.cli as cli_mod
import its_still_here.core.pipeline as pipeline_mod
from its_still_here.config import AppConfig, RuleConfig, SourceConfig, StorageConfig, load_config
from its_still_here.core.db import StateDB
from its_still_here.core.models import FileEvent, TaskStatus, TransferTask
from its_still_here.core.pipeline import SyncPipeline
from its_still_here.storages import BaseStorageProvider


# =====================================================================
# Helpers
# =====================================================================


def _make_config(
    tmp_path: Path,
    *,
    mode: str = "move",
    max_retries: int = 3,
    target_name: str = "mock_webdav",
    storage_type: str = "webdav",
    rule_enabled: bool = True,
) -> AppConfig:
    """Build an AppConfig rooted inside *tmp_path*."""
    watch_dir = tmp_path / "inbox"
    watch_dir.mkdir(parents=True, exist_ok=True)
    return AppConfig(
        staging_dir=tmp_path / "staging",
        db_path=tmp_path / "data" / "state.db",
        retry_interval_seconds=60,
        max_retries=max_retries,
        sources=[
            SourceConfig(
                name="local_inbox",
                type="folder",
                enabled=True,
                watch_dir=watch_dir,
            )
        ],
        storages=[
            StorageConfig(
                name=target_name,
                type=storage_type,
                enabled=True,
                base_url="https://dav.example.com/dav",
                username="tester",
                password="secret",
            )
        ],
        rules=[
            RuleConfig(
                name="archive_all",
                enabled=rule_enabled,
                target=target_name,
                path_template="{source}/{chat_name}/{filename}",
                mode=mode,
            )
        ],
    )


def _make_source_file(
    tmp_path: Path,
    name: str = "report.pdf",
    content: bytes = b"hello-its-still-here",
) -> Path:
    """Create a real source file on disk and return its path."""
    file_path = tmp_path / "inbox" / name
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(content)
    return file_path


def _make_event(
    file_path: Path,
    *,
    event_id: str = "evt-1",
    source_name: str = "local_inbox",
    chat_name: str = "ProjectGroup",
    sender: str = "Alice",
) -> FileEvent:
    """Construct a FileEvent pointing at an existing file."""
    return FileEvent(
        event_id=event_id,
        source_name=source_name,
        chat_name=chat_name,
        sender=sender,
        file_name=file_path.name,
        file_path=file_path,
        file_size=file_path.stat().st_size,
        created_at=datetime(2026, 11, 26, 12, 0, 0, tzinfo=timezone.utc),
    )


def _wait_until_settled(
    db: StateDB,
    expected_count: int = 1,
    terminal_statuses: frozenset[TaskStatus] = frozenset(
        {
            TaskStatus.VERIFIED,
            TaskStatus.CLEANED,
            TaskStatus.PENDING_RETRY,
            TaskStatus.FAILED,
        }
    ),
    timeout: float = 5.0,
) -> list[TransferTask]:
    """Poll StateDB until *expected_count* tasks reach a settled status."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        tasks = db.list_recent_tasks(limit=50)
        if len(tasks) >= expected_count and all(
            t.status in terminal_statuses for t in tasks
        ):
            return tasks
        time.sleep(0.01)
    raise TimeoutError("Timed out waiting for pipeline tasks to settle.")


def _patch_mock_storage(
    monkeypatch: pytest.MonkeyPatch,
    storage_type: str = "webdav",
    provider: MagicMock | None = None,
) -> MagicMock:
    """Register a mocked BaseStorageProvider in SyncPipeline._STORAGE_PROVIDERS."""
    mock_provider = provider or MagicMock(spec=BaseStorageProvider)
    monkeypatch.setitem(
        pipeline_mod._STORAGE_PROVIDERS,
        storage_type,
        lambda _cfg: mock_provider,
    )
    return mock_provider


# =====================================================================
# SyncPipeline End-to-End Tests
# =====================================================================


class TestSyncPipelineEndToEnd:
    """End-to-end tests for SyncPipeline with a mocked BaseStorageProvider."""

    def test_handle_event_stages_uploads_verifies_cleans_and_deduplicates(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """In move mode, handle_event stages, uploads, verifies, sets CLEANED,
        deletes the staged file, and skips duplicate events with the same SHA256."""
        payload = b"important-archive-content-12345"
        expected_hash = hashlib.sha256(payload).hexdigest()
        source_file = _make_source_file(tmp_path, name="contract.pdf", content=payload)
        cfg = _make_config(tmp_path, mode="move")

        staged_snapshots: list[tuple[Path, str, bool, bytes]] = []
        mock_provider = MagicMock(spec=BaseStorageProvider)

        def fake_upload(local_path: Path, remote_path: str) -> bool:
            staged_snapshots.append(
                (local_path, remote_path, local_path.exists(), local_path.read_bytes())
            )
            return True

        mock_provider.upload.side_effect = fake_upload
        mock_provider.verify.return_value = True
        _patch_mock_storage(monkeypatch, "webdav", mock_provider)

        pipeline = SyncPipeline(cfg, max_workers=2)
        try:
            event1 = _make_event(source_file, event_id="evt-1")
            pipeline.handle_event(event1)

            tasks = _wait_until_settled(
                pipeline.db,
                expected_count=1,
                terminal_statuses=frozenset({TaskStatus.CLEANED, TaskStatus.FAILED}),
            )
            assert len(tasks) == 1
            task = tasks[0]

            # 1. File was staged into staging_dir/<task_id>/<file_name> before upload
            assert len(staged_snapshots) == 1
            staged_path, remote_path, existed_at_upload, uploaded_bytes = staged_snapshots[0]
            assert staged_path != source_file.resolve()
            assert staged_path.parent.parent == cfg.staging_dir.resolve()
            assert staged_path.name == "contract.pdf"
            assert remote_path == "local_inbox/ProjectGroup/contract.pdf"
            assert existed_at_upload is True
            assert uploaded_bytes == payload

            # 2. Provider upload & verify called with staged path and rendered remote path
            mock_provider.upload.assert_called_once_with(staged_path, remote_path)
            mock_provider.verify.assert_called_once_with(staged_path, remote_path)

            # 3. Task status is CLEANED and staged file + empty task dir are removed
            assert task.file_hash == expected_hash
            assert task.status == TaskStatus.CLEANED
            assert task.retry_count == 0
            assert not staged_path.exists()
            assert not staged_path.parent.exists()
            assert source_file.exists()

            # 4. Repeat event with the exact same SHA256 (even via another copy) is deduplicated
            duplicate_copy = _make_source_file(
                tmp_path, name="contract_copy.pdf", content=payload
            )
            event2 = _make_event(duplicate_copy, event_id="evt-2")
            pipeline.handle_event(event2)
        finally:
            pipeline.stop()

        # Verify persisted DB state after graceful shutdown
        db = StateDB(cfg.db_path)
        try:
            all_tasks = db.list_recent_tasks(limit=10)
            assert len(all_tasks) == 1
            assert all_tasks[0].status == TaskStatus.CLEANED
        finally:
            db.close()

        assert mock_provider.upload.call_count == 1
        assert mock_provider.verify.call_count == 1

    def test_handle_event_copy_mode_keeps_staged_file_and_sets_verified(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """In copy mode, status stops at VERIFIED and the staged file is retained."""
        source_file = _make_source_file(tmp_path, name="notes.txt", content=b"keep-me")
        cfg = _make_config(tmp_path, mode="copy")

        mock_provider = _patch_mock_storage(monkeypatch, "webdav")
        mock_provider.upload.return_value = True
        mock_provider.verify.return_value = True

        pipeline = SyncPipeline(cfg, max_workers=1)
        try:
            pipeline.handle_event(_make_event(source_file))
        finally:
            pipeline.stop()

        db = StateDB(cfg.db_path)
        try:
            tasks = db.list_recent_tasks(limit=10)
            assert len(tasks) == 1
            task = tasks[0]
            assert task.status == TaskStatus.VERIFIED
            assert task.local_path.exists()
            assert task.local_path.read_bytes() == b"keep-me"
        finally:
            db.close()


# =====================================================================
# SyncPipeline Retry & Failure Tests
# =====================================================================


class TestSyncPipelineRetryPath:
    """Tests for upload/verify failures and retry state transitions."""

    def test_upload_returns_false_sets_pending_retry_with_retry_count_1(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """When provider.upload returns False, task moves to PENDING_RETRY with retry_count == 1."""
        source_file = _make_source_file(tmp_path, name="retry_me.bin", content=b"retry-data")
        cfg = _make_config(tmp_path, mode="move", max_retries=3)

        mock_provider = _patch_mock_storage(monkeypatch, "webdav")
        mock_provider.upload.return_value = False
        mock_provider.verify.return_value = True

        pipeline = SyncPipeline(cfg, max_workers=1)
        try:
            pipeline.handle_event(_make_event(source_file))
        finally:
            pipeline.stop()

        mock_provider.upload.assert_called_once()
        mock_provider.verify.assert_not_called()

        db = StateDB(cfg.db_path)
        try:
            tasks = db.list_recent_tasks(limit=10)
            assert len(tasks) == 1
            task = tasks[0]
            assert task.status == TaskStatus.PENDING_RETRY
            assert task.retry_count == 1
            assert task.error_message is not None
            assert "Upload failed" in task.error_message
            # Staged file must be preserved for future retry attempts
            assert task.local_path.exists()
        finally:
            db.close()

    def test_upload_returns_false_at_max_retries_sets_failed(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """When retry_count + 1 reaches max_retries, task transitions to FAILED."""
        source_file = _make_source_file(tmp_path, name="fail_fast.bin", content=b"fail-data")
        cfg = _make_config(tmp_path, mode="move", max_retries=1)

        mock_provider = _patch_mock_storage(monkeypatch, "webdav")
        mock_provider.upload.return_value = False

        pipeline = SyncPipeline(cfg, max_workers=1)
        try:
            pipeline.handle_event(_make_event(source_file))
        finally:
            pipeline.stop()

        db = StateDB(cfg.db_path)
        try:
            tasks = db.list_recent_tasks(limit=10)
            assert len(tasks) == 1
            assert tasks[0].status == TaskStatus.FAILED
            assert tasks[0].retry_count == 1
        finally:
            db.close()


# =====================================================================
# CLI Tests (init, check, status)
# =====================================================================


runner = CliRunner()


def _write_cli_config(
    config_path: Path,
    *,
    watch_dir: Path,
    db_path: Path,
    staging_dir: Path,
) -> None:
    """Write a minimal valid YAML config for CLI tests."""
    data = {
        "staging_dir": str(staging_dir),
        "db_path": str(db_path),
        "retry_interval_seconds": 60,
        "max_retries": 5,
        "sources": [
            {
                "name": "local_inbox",
                "type": "folder",
                "enabled": True,
                "watch_dir": str(watch_dir),
            }
        ],
        "storages": [
            {
                "name": "mock_webdav",
                "type": "webdav",
                "enabled": True,
                "base_url": "https://dav.example.com/dav",
                "username": "u",
                "password": "p",
            },
            {
                "name": "disabled_rclone",
                "type": "rclone",
                "enabled": False,
                "remote_name": "unused",
            },
        ],
        "rules": [
            {
                "name": "archive_all",
                "enabled": True,
                "target": "mock_webdav",
                "mode": "move",
            }
        ],
    }
    config_path.write_text(
        yaml.safe_dump(data, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


class TestCLI:
    """Tests for CLI commands: init, check, status."""

    def test_cli_init_creates_valid_yaml_and_respects_force(
        self,
        tmp_path: Path,
    ) -> None:
        """init writes a parseable config, refuses overwrite without --force, and overwrites with --force."""
        out_path = tmp_path / "nested" / "config.yaml"

        res_create = runner.invoke(cli_mod.app, ["init", "--output", str(out_path)])
        assert res_create.exit_code == 0, res_create.output
        assert out_path.is_file()
        loaded = load_config(out_path)
        assert isinstance(loaded, AppConfig)

        # Without --force, existing file must fail with exit code 1 and remain untouched
        out_path.write_text("custom: sentinel\n", encoding="utf-8")
        res_exists = runner.invoke(cli_mod.app, ["init", "-o", str(out_path)])
        assert res_exists.exit_code == 1
        assert out_path.read_text(encoding="utf-8") == "custom: sentinel\n"

        # With --force, existing file is overwritten
        res_force = runner.invoke(cli_mod.app, ["init", "-o", str(out_path), "--force"])
        assert res_force.exit_code == 0, res_force.output
        reloaded = load_config(out_path)
        assert isinstance(reloaded, AppConfig)

    def test_cli_check_passes_when_healthy_and_fails_when_unhealthy(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """check exits 0 when all enabled backends are healthy and exits 1 if any fails."""
        watch_dir = tmp_path / "inbox"
        watch_dir.mkdir()
        cfg_path = tmp_path / "config.yaml"
        _write_cli_config(
            cfg_path,
            watch_dir=watch_dir,
            db_path=tmp_path / "data" / "state.db",
            staging_dir=tmp_path / "staging",
        )

        mock_provider = MagicMock(spec=BaseStorageProvider)
        mock_provider.check_health.return_value = (True, "OK")
        monkeypatch.setitem(
            cli_mod._STORAGE_PROVIDERS,
            "webdav",
            lambda _cfg: mock_provider,
        )

        res_ok = runner.invoke(cli_mod.app, ["check", "--config", str(cfg_path)])
        assert res_ok.exit_code == 0, res_ok.output
        assert "OK" in res_ok.output
        assert "local_inbox" in res_ok.output
        assert "mock_webdav" in res_ok.output

        # Make storage health check fail -> exit code 1
        mock_provider.check_health.return_value = (False, "connection refused")
        res_fail = runner.invoke(cli_mod.app, ["check", "--config", str(cfg_path)])
        assert res_fail.exit_code == 1
        assert "FAIL" in res_fail.output

        # Missing config file -> exit code 1
        res_missing = runner.invoke(
            cli_mod.app, ["check", "--config", str(tmp_path / "no_such.yaml")]
        )
        assert res_missing.exit_code == 1

    def test_cli_status_displays_recent_tasks_with_limit(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """status prints recent tasks from StateDB ordered by updated_at DESC up to --limit."""
        watch_dir = tmp_path / "inbox"
        watch_dir.mkdir()
        db_path = tmp_path / "data" / "state.db"
        cfg_path = tmp_path / "config.yaml"
        _write_cli_config(
            cfg_path,
            watch_dir=watch_dir,
            db_path=db_path,
            staging_dir=tmp_path / "staging",
        )

        # Widen console so Rich Table does not truncate filenames in CliRunner
        monkeypatch.setattr(cli_mod.console, "width", 200, raising=False)

        # Empty DB succeeds cleanly
        res_empty = runner.invoke(cli_mod.app, ["status", "--config", str(cfg_path)])
        assert res_empty.exit_code == 0, res_empty.output

        db = StateDB(db_path)
        try:
            for idx, (status, err) in enumerate(
                [
                    (TaskStatus.CLEANED, None),
                    (TaskStatus.PENDING_RETRY, "timeout"),
                    (TaskStatus.VERIFIED, None),
                ],
                start=1,
            ):
                ts = datetime(2026, 11, 26, 10, idx, 0, tzinfo=timezone.utc)
                db.upsert_task(
                    TransferTask(
                        task_id=f"task-{idx:04d}-abcdef",
                        file_hash=f"hash-{idx}",
                        source_name="local_inbox",
                        chat_name="TeamChat",
                        file_name=f"file_{idx}.pdf",
                        local_path=tmp_path / "staging" / f"file_{idx}.pdf",
                        remote_target="mock_webdav",
                        remote_path=f"local_inbox/TeamChat/file_{idx}.pdf",
                        status=status,
                        retry_count=1 if status == TaskStatus.PENDING_RETRY else 0,
                        error_message=err,
                        created_at=ts,
                        updated_at=ts,
                    )
                )
        finally:
            db.close()

        res_limit = runner.invoke(
            cli_mod.app, ["status", "--config", str(cfg_path), "--limit", "2"]
        )
        assert res_limit.exit_code == 0, res_limit.output
        assert "file_3.pdf" in res_limit.output
        assert "file_2.pdf" in res_limit.output
        assert "file_1.pdf" not in res_limit.output
        assert "VERIFIED" in res_limit.output
        assert "PENDING_RETRY" in res_limit.output

        # Missing config -> exit code 1
        res_missing = runner.invoke(
            cli_mod.app, ["status", "--config", str(tmp_path / "missing.yaml")]
        )
        assert res_missing.exit_code == 1
