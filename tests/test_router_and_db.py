"""Unit tests for RuleEngine and StateDB in its-still-here."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from its_still_here.config import RuleConfig
from its_still_here.core.db import StateDB
from its_still_here.core.models import FileEvent, TaskStatus, TransferTask
from its_still_here.core.router import (
    RuleEngine,
    _normalize_ext,
    _render_path,
    _sanitize_segment,
)


# =====================================================================
# Fixtures & Helpers
# =====================================================================


def _make_event(
    *,
    source_name: str = "test_source",
    chat_name: str = "ProjectChat",
    sender: str = "Alice",
    file_name: str = "report.pdf",
    file_size: int = 1024 * 1024,  # 1 MiB
    created_at: datetime | None = None,
) -> FileEvent:
    """Helper to construct a FileEvent with reasonable defaults."""
    return FileEvent(
        event_id="evt_001",
        source_name=source_name,
        chat_name=chat_name,
        sender=sender,
        file_name=file_name,
        file_path=Path(f"/local/staging/{file_name}"),
        file_size=file_size,
        created_at=created_at or datetime(2026, 11, 26, 14, 30, 45, tzinfo=timezone.utc),
    )


def _make_task(
    *,
    task_id: str = "task_001",
    file_hash: str = "hash_abc123",
    source_name: str = "folder_source",
    chat_name: str = "DevGroup",
    file_name: str = "document.docx",
    local_path: str = "/tmp/staging/document.docx",
    remote_target: str = "my_gdrive",
    remote_path: str = "backup/DevGroup/document.docx",
    status: TaskStatus = TaskStatus.DETECTED,
    retry_count: int = 0,
    error_message: str | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
) -> TransferTask:
    """Helper to construct a TransferTask with reasonable defaults."""
    now = datetime(2026, 11, 26, 10, 0, 0, tzinfo=timezone.utc)
    return TransferTask(
        task_id=task_id,
        file_hash=file_hash,
        source_name=source_name,
        chat_name=chat_name,
        file_name=file_name,
        local_path=Path(local_path),
        remote_target=remote_target,
        remote_path=remote_path,
        status=status,
        retry_count=retry_count,
        error_message=error_message,
        created_at=created_at or now,
        updated_at=updated_at or now,
    )


# =====================================================================
# RuleEngine & Routing Tests
# =====================================================================


class TestRuleEngineMatching:
    """Tests for RuleEngine routing filter logic."""

    def test_include_chats_filter(self) -> None:
        """Rule with include_chats only matches listed chats; empty list matches all."""
        rule_specific = RuleConfig(
            name="specific_chats",
            target="remote_backup",
            include_chats=["FinanceGroup", "DevTeam"],
        )
        rule_all = RuleConfig(
            name="all_chats",
            target="remote_backup",
            include_chats=[],
        )
        engine = RuleEngine([rule_specific, rule_all])

        # Chat in list matches both
        matches_dev = engine.match(_make_event(chat_name="DevTeam"))
        assert [r.name for r, _ in matches_dev] == ["specific_chats", "all_chats"]

        # Chat not in list only matches rule_all
        matches_hr = engine.match(_make_event(chat_name="HRGroup"))
        assert [r.name for r, _ in matches_hr] == ["all_chats"]

    def test_exclude_chats_filter(self) -> None:
        """exclude_chats takes precedence and drops matching events."""
        rule = RuleConfig(
            name="exclude_spam",
            target="remote_backup",
            include_chats=["FinanceGroup", "SpamChannel"],
            exclude_chats=["SpamChannel", "MutedGroup"],
        )
        engine = RuleEngine([rule])

        # Excluded chat must be dropped even if in include_chats
        assert engine.match(_make_event(chat_name="SpamChannel")) == []

        # Non-excluded chat in include_chats matches
        matches = engine.match(_make_event(chat_name="FinanceGroup"))
        assert len(matches) == 1
        assert matches[0][0].name == "exclude_spam"

    def test_include_exts_filter(self) -> None:
        """include_exts normalizes case and leading dots; empty matches all."""
        rule = RuleConfig(
            name="docs_only",
            target="remote_backup",
            include_exts=[".PDF", "docx", ".XLSX"],
        )
        engine = RuleEngine([rule])

        # Case-insensitive match with dot in config
        assert len(engine.match(_make_event(file_name="invoice.pdf"))) == 1
        assert len(engine.match(_make_event(file_name="INVOICE.PDF"))) == 1

        # Match without dot in config
        assert len(engine.match(_make_event(file_name="spec.docx"))) == 1

        # Disallowed extension fails match
        assert engine.match(_make_event(file_name="archive.zip")) == []

    def test_exclude_exts_filter(self) -> None:
        """exclude_exts drops files with matching extensions."""
        rule = RuleConfig(
            name="no_executables",
            target="remote_backup",
            include_exts=[],
            exclude_exts=[".exe", "BAT", ".sh"],
        )
        engine = RuleEngine([rule])

        assert engine.match(_make_event(file_name="setup.exe")) == []
        assert engine.match(_make_event(file_name="script.BAT")) == []
        assert engine.match(_make_event(file_name="deploy.sh")) == []

        # Other extensions pass
        assert len(engine.match(_make_event(file_name="document.pdf"))) == 1

    def test_size_bounds_filtering(self) -> None:
        """min_size_mb and max_size_mb filter by megabytes (1024 * 1024 bytes)."""
        mb = 1024 * 1024
        rule = RuleConfig(
            name="size_bounded",
            target="remote_backup",
            min_size_mb=2.0,
            max_size_mb=10.0,
        )
        engine = RuleEngine([rule])

        # Below min size (1 MiB < 2 MiB)
        assert engine.match(_make_event(file_size=1 * mb)) == []

        # Exactly at min size (2 MiB)
        assert len(engine.match(_make_event(file_size=2 * mb))) == 1

        # In between (5 MiB)
        assert len(engine.match(_make_event(file_size=5 * mb))) == 1

        # Exactly at max size (10 MiB)
        assert len(engine.match(_make_event(file_size=10 * mb))) == 1

        # Above max size (11 MiB > 10 MiB)
        assert engine.match(_make_event(file_size=11 * mb)) == []

    def test_sources_filter_and_disabled_rules(self) -> None:
        """Disabled rules and mismatched sources are skipped."""
        rule_disabled = RuleConfig(
            name="disabled_rule",
            target="remote_backup",
            enabled=False,
        )
        rule_folder = RuleConfig(
            name="folder_only",
            target="remote_backup",
            sources=["folder_source"],
        )
        engine = RuleEngine([rule_disabled, rule_folder])

        # Matches correct source
        assert len(engine.match(_make_event(source_name="folder_source"))) == 1

        # Mismatched source skipped
        assert engine.match(_make_event(source_name="wechat_source")) == []


# =====================================================================
# Path Template Rendering & Path Traversal Sanitization Tests
# =====================================================================


class TestPathTemplateAndSanitization:
    """Tests for path template placeholder substitution and traversal defense."""

    def test_sanitize_segment_unit(self) -> None:
        """_sanitize_segment replaces dangerous characters and collapses dots."""
        # Forbidden characters replaced by underscore
        assert _sanitize_segment('foo/bar\\baz:tag<val>|pipe?star*"') == "foo_bar_baz_tag_val__pipe_star__"

        # Traversal dot segments collapsed to single underscore
        assert _sanitize_segment("..") == "_"
        assert _sanitize_segment(".") == "_"
        assert _sanitize_segment("") == "_"
        assert _sanitize_segment("   ") == "_"

        # Valid normal strings preserved
        assert _sanitize_segment("Normal-Chat_123") == "Normal-Chat_123"

    def test_path_template_all_placeholders(self) -> None:
        """All supported placeholders are accurately substituted in UTC."""
        dt = datetime(2026, 7, 4, 9, 5, 8, tzinfo=timezone.utc)
        event = _make_event(
            source_name="wechat_main",
            chat_name="Work Team",
            sender="Bob Smith",
            file_name="Quarterly_Report.final.pdf",
            created_at=dt,
        )
        rule = RuleConfig(
            name="test_tmpl",
            target="cloud",
            path_template=(
                "{source}/{chat_name}/{sender}/"
                "{YYYY}/{MM}/{DD}/{HH}_{mm}_{ss}/"
                "{stem}_backup.{ext}"
            ),
        )

        rendered = _render_path(rule.path_template, rule, event)
        expected = (
            "wechat_main/Work Team/Bob Smith/2026/07/04/09_05_08/Quarterly_Report.final_backup.pdf"
        )
        assert rendered == expected

    def test_path_template_default_sender_when_empty(self) -> None:
        """When sender is empty string, placeholder defaults to '_'."""
        event = _make_event(sender="")
        rule = RuleConfig(
            name="sender_check",
            target="cloud",
            path_template="archive/{sender}/{filename}",
        )
        rendered = _render_path(rule.path_template, rule, event)
        assert rendered == "archive/_/report.pdf"

    def test_path_traversal_attack_in_chat_name(self) -> None:
        """Directory traversal attempts via chat_name are neutralized."""
        malicious_chat = "../../etc/shadow"
        event = _make_event(chat_name=malicious_chat, file_name="secret.txt")
        rule = RuleConfig(
            name="traversal_test",
            target="cloud",
            path_template="{chat_name}/{filename}",
        )
        rendered = _render_path(rule.path_template, rule, event)

        # Slashes and dots in chat_name are sanitized, preventing '../' traversal
        assert ".." not in rendered
        assert not rendered.startswith("/")
        assert rendered == "____etc_shadow/secret.txt"

    def test_path_traversal_with_pure_dots_in_sender(self) -> None:
        """Sender with '..' segment alone is converted to '_'."""
        event = _make_event(sender="..")
        rule = RuleConfig(
            name="dot_sender",
            target="cloud",
            path_template="users/{sender}/{filename}",
        )
        rendered = _render_path(rule.path_template, rule, event)
        assert rendered == "users/_/report.pdf"


# =====================================================================
# StateDB Persistence Tests
# =====================================================================


class TestStateDB:
    """Tests for SQLite StateDB operations."""

    @pytest.fixture
    def db(self, tmp_path: Path) -> StateDB:
        """Fixture that provides a fresh StateDB backed by a temp directory."""
        db_file = tmp_path / "data" / "state.db"
        state_db = StateDB(db_file)
        yield state_db
        state_db.close()

    def test_upsert_and_get_task(self, db: StateDB) -> None:
        """upsert_task creates and subsequent upsert updates the record."""
        task = _make_task(task_id="t1", status=TaskStatus.DETECTED)
        db.upsert_task(task)

        fetched = db.get_task("t1")
        assert fetched is not None
        assert fetched.task_id == "t1"
        assert fetched.file_hash == "hash_abc123"
        assert fetched.status == TaskStatus.DETECTED
        assert fetched.retry_count == 0

        # Update task via upsert
        updated_task = task.model_copy(
            update={"status": TaskStatus.UPLOADING, "retry_count": 2, "error_message": "transient"}
        )
        db.upsert_task(updated_task)

        refetched = db.get_task("t1")
        assert refetched is not None
        assert refetched.status == TaskStatus.UPLOADING
        assert refetched.retry_count == 2
        assert refetched.error_message == "transient"

    def test_get_nonexistent_task_returns_none(self, db: StateDB) -> None:
        """Querying a non-existent task_id returns None."""
        assert db.get_task("no_such_task") is None

    def test_exists_completed(self, db: StateDB) -> None:
        """exists_completed returns True only for VERIFIED or CLEANED states with matching hash and target."""
        # Non-completed statuses return False
        db.upsert_task(_make_task(task_id="t_detected", file_hash="h1", remote_target="r1", status=TaskStatus.DETECTED))
        db.upsert_task(_make_task(task_id="t_uploading", file_hash="h2", remote_target="r1", status=TaskStatus.UPLOADING))
        db.upsert_task(_make_task(task_id="t_retry", file_hash="h3", remote_target="r1", status=TaskStatus.PENDING_RETRY))
        db.upsert_task(_make_task(task_id="t_failed", file_hash="h4", remote_target="r1", status=TaskStatus.FAILED))

        assert db.exists_completed("h1", "r1") is False
        assert db.exists_completed("h2", "r1") is False
        assert db.exists_completed("h3", "r1") is False
        assert db.exists_completed("h4", "r1") is False

        # VERIFIED returns True
        db.upsert_task(_make_task(task_id="t_verified", file_hash="h5", remote_target="r1", status=TaskStatus.VERIFIED))
        assert db.exists_completed("h5", "r1") is True

        # CLEANED returns True
        db.upsert_task(_make_task(task_id="t_cleaned", file_hash="h6", remote_target="r1", status=TaskStatus.CLEANED))
        assert db.exists_completed("h6", "r1") is True

        # Different target returns False
        assert db.exists_completed("h5", "different_target") is False

    def test_update_status_without_retry(self, db: StateDB) -> None:
        """update_status updates status and error_message without changing retry_count."""
        db.upsert_task(_make_task(task_id="t_status", status=TaskStatus.DETECTED, retry_count=1))

        db.update_status("t_status", TaskStatus.UPLOADING, error_message=None, increment_retry=False)
        task = db.get_task("t_status")
        assert task is not None
        assert task.status == TaskStatus.UPLOADING
        assert task.retry_count == 1
        assert task.error_message is None

    def test_update_status_with_increment_retry(self, db: StateDB) -> None:
        """update_status with increment_retry=True atomically increments retry_count."""
        db.upsert_task(_make_task(task_id="t_retry_inc", status=TaskStatus.UPLOADING, retry_count=0))

        db.update_status(
            "t_retry_inc",
            TaskStatus.PENDING_RETRY,
            error_message="Connection timed out",
            increment_retry=True,
        )
        task = db.get_task("t_retry_inc")
        assert task is not None
        assert task.status == TaskStatus.PENDING_RETRY
        assert task.retry_count == 1
        assert task.error_message == "Connection timed out"

        # Increment again
        db.update_status(
            "t_retry_inc",
            TaskStatus.PENDING_RETRY,
            error_message="Host unreachable",
            increment_retry=True,
        )
        task = db.get_task("t_retry_inc")
        assert task is not None
        assert task.retry_count == 2
        assert task.error_message == "Host unreachable"

    def test_list_pending_tasks(self, db: StateDB) -> None:
        """list_pending_tasks returns only PENDING_RETRY tasks with retry_count < max_retries ordered by updated_at ASC."""
        t1 = _make_task(
            task_id="t_p1",
            status=TaskStatus.PENDING_RETRY,
            retry_count=1,
            updated_at=datetime(2026, 11, 26, 8, 0, 0, tzinfo=timezone.utc),
        )
        t2 = _make_task(
            task_id="t_p2",
            status=TaskStatus.PENDING_RETRY,
            retry_count=3,
            updated_at=datetime(2026, 11, 26, 9, 0, 0, tzinfo=timezone.utc),
        )
        t_maxed = _make_task(
            task_id="t_maxed",
            status=TaskStatus.PENDING_RETRY,
            retry_count=5,  # Equal to max_retries -> should not be included
            updated_at=datetime(2026, 11, 26, 7, 0, 0, tzinfo=timezone.utc),
        )
        t_done = _make_task(
            task_id="t_done",
            status=TaskStatus.VERIFIED,
            retry_count=0,
            updated_at=datetime(2026, 11, 26, 6, 0, 0, tzinfo=timezone.utc),
        )
        for t in [t1, t2, t_maxed, t_done]:
            db.upsert_task(t)

        pending = db.list_pending_tasks(max_retries=5)
        # Only t1 and t2 qualify; ordered by updated_at ASC (t1 at 8:00 before t2 at 9:00)
        assert [t.task_id for t in pending] == ["t_p1", "t_p2"]

    def test_list_recent_tasks(self, db: StateDB) -> None:
        """list_recent_tasks returns tasks ordered by updated_at DESC up to limit."""
        for i in range(1, 6):
            task = _make_task(
                task_id=f"t_{i}",
                status=TaskStatus.DETECTED,
                updated_at=datetime(2026, 11, 26, 10, i, 0, tzinfo=timezone.utc),
            )
            db.upsert_task(task)

        recent = db.list_recent_tasks(limit=3)
        assert len(recent) == 3
        # Most recent first: t_5 (10:05), t_4 (10:04), t_3 (10:03)
        assert [t.task_id for t in recent] == ["t_5", "t_4", "t_3"]
