"""SQLite-backed state store for :class:`TransferTask` persistence."""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from .models import TaskStatus, TransferTask


_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id         TEXT PRIMARY KEY,
    file_hash       TEXT NOT NULL,
    source_name     TEXT NOT NULL,
    chat_name       TEXT NOT NULL,
    file_name       TEXT NOT NULL,
    local_path      TEXT NOT NULL,
    remote_target   TEXT NOT NULL,
    remote_path     TEXT NOT NULL,
    status          TEXT NOT NULL,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    error_message   TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_hash_target
    ON tasks (file_hash, remote_target);
CREATE INDEX IF NOT EXISTS idx_tasks_status
    ON tasks (status);
"""

_UPSERT_SQL = """
INSERT INTO tasks (
    task_id, file_hash, source_name, chat_name, file_name,
    local_path, remote_target, remote_path, status,
    retry_count, error_message, created_at, updated_at
) VALUES (
    :task_id, :file_hash, :source_name, :chat_name, :file_name,
    :local_path, :remote_target, :remote_path, :status,
    :retry_count, :error_message, :created_at, :updated_at
)
ON CONFLICT(task_id) DO UPDATE SET
    file_hash     = excluded.file_hash,
    source_name   = excluded.source_name,
    chat_name     = excluded.chat_name,
    file_name     = excluded.file_name,
    local_path    = excluded.local_path,
    remote_target = excluded.remote_target,
    remote_path   = excluded.remote_path,
    status        = excluded.status,
    retry_count   = excluded.retry_count,
    error_message = excluded.error_message,
    created_at    = excluded.created_at,
    updated_at    = excluded.updated_at;
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_to_task(row: sqlite3.Row) -> TransferTask:
    """Convert a database row to a :class:`TransferTask`."""
    return TransferTask(
        task_id=row["task_id"],
        file_hash=row["file_hash"],
        source_name=row["source_name"],
        chat_name=row["chat_name"],
        file_name=row["file_name"],
        local_path=Path(row["local_path"]),
        remote_target=row["remote_target"],
        remote_path=row["remote_path"],
        status=TaskStatus(row["status"]),
        retry_count=row["retry_count"],
        error_message=row["error_message"],
        created_at=datetime.fromisoformat(row["created_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
    )


class StateDB:
    """Thread-safe SQLite state store for :class:`TransferTask` records.

    Args:
        db_path: Path to the SQLite database file.
                 Parent directories are created automatically.
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._lock = threading.Lock()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(db_path),
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_CREATE_TABLE_SQL)
        self._conn.commit()

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def upsert_task(self, task: TransferTask) -> None:
        """Insert or fully replace a task record.

        Args:
            task: The :class:`TransferTask` to persist.
        """
        params = {
            "task_id": task.task_id,
            "file_hash": task.file_hash,
            "source_name": task.source_name,
            "chat_name": task.chat_name,
            "file_name": task.file_name,
            "local_path": str(task.local_path),
            "remote_target": task.remote_target,
            "remote_path": task.remote_path,
            "status": task.status.value,
            "retry_count": task.retry_count,
            "error_message": task.error_message,
            "created_at": task.created_at.isoformat(),
            "updated_at": task.updated_at.isoformat(),
        }
        with self._lock:
            self._conn.execute(_UPSERT_SQL, params)
            self._conn.commit()

    def update_status(
        self,
        task_id: str,
        status: TaskStatus,
        error_message: str | None = None,
        increment_retry: bool = False,
    ) -> None:
        """Update the status (and optionally retry counter) of a task.

        Args:
            task_id: Identifier of the task to update.
            status: New :class:`TaskStatus` value.
            error_message: Optional error detail to store.
            increment_retry: When ``True``, atomically increment ``retry_count``.
        """
        now = _now_iso()
        with self._lock:
            if increment_retry:
                self._conn.execute(
                    """UPDATE tasks
                          SET status = ?, error_message = ?,
                              retry_count = retry_count + 1,
                              updated_at = ?
                        WHERE task_id = ?""",
                    (status.value, error_message, now, task_id),
                )
            else:
                self._conn.execute(
                    """UPDATE tasks
                          SET status = ?, error_message = ?, updated_at = ?
                        WHERE task_id = ?""",
                    (status.value, error_message, now, task_id),
                )
            self._conn.commit()

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get_task(self, task_id: str) -> TransferTask | None:
        """Fetch a single task by its identifier.

        Args:
            task_id: Primary key of the task.

        Returns:
            The :class:`TransferTask` if found, otherwise ``None``.
        """
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            )
            row = cur.fetchone()
        return _row_to_task(row) if row else None

    def exists_completed(self, file_hash: str, remote_target: str) -> bool:
        """Check whether a file has already been successfully uploaded.

        A task is considered completed when its status is
        :attr:`TaskStatus.VERIFIED` or :attr:`TaskStatus.CLEANED`.

        Args:
            file_hash: SHA-256 hex digest of the file.
            remote_target: Name of the remote storage.

        Returns:
            ``True`` if a completed task exists for this hash + target pair.
        """
        completed = (TaskStatus.VERIFIED.value, TaskStatus.CLEANED.value)
        with self._lock:
            cur = self._conn.execute(
                """SELECT 1 FROM tasks
                    WHERE file_hash = ? AND remote_target = ?
                      AND status IN (?, ?)
                    LIMIT 1""",
                (file_hash, remote_target, *completed),
            )
            return cur.fetchone() is not None

    def list_pending_tasks(self, max_retries: int) -> list[TransferTask]:
        """Return tasks that are eligible for a retry attempt.

        Tasks with status :attr:`TaskStatus.PENDING_RETRY` whose
        ``retry_count`` is strictly less than *max_retries* are returned,
        ordered by ``updated_at`` ascending (oldest first).

        Args:
            max_retries: Maximum number of allowed retry attempts.

        Returns:
            List of matching :class:`TransferTask` objects.
        """
        with self._lock:
            cur = self._conn.execute(
                """SELECT * FROM tasks
                    WHERE status = ? AND retry_count < ?
                    ORDER BY updated_at ASC""",
                (TaskStatus.PENDING_RETRY.value, max_retries),
            )
            rows = cur.fetchall()
        return [_row_to_task(r) for r in rows]

    def list_recent_tasks(self, limit: int = 20) -> list[TransferTask]:
        """Return the most recently updated tasks.

        Args:
            limit: Maximum number of tasks to return (default: 20).

        Returns:
            List of :class:`TransferTask` objects ordered by ``updated_at`` descending.
        """
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM tasks ORDER BY updated_at DESC LIMIT ?",
                (limit,),
            )
            rows = cur.fetchall()
        return [_row_to_task(r) for r in rows]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Flush and close the underlying database connection."""
        with self._lock:
            self._conn.close()
