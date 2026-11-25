"""Domain models for file events and transfer tasks."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    """Lifecycle states of a :class:`TransferTask`."""

    DETECTED = "DETECTED"
    DOWNLOADING = "DOWNLOADING"
    STAGED = "STAGED"
    UPLOADING = "UPLOADING"
    VERIFIED = "VERIFIED"
    CLEANED = "CLEANED"
    PENDING_RETRY = "PENDING_RETRY"
    FAILED = "FAILED"


class FileEvent(BaseModel):
    """An immutable record of a file detected by a source watcher."""

    event_id: str
    """Unique identifier for this event (e.g. ``uuid4().hex``)."""

    source_name: str
    """Name of the source that emitted this event (matches :attr:`SourceConfig.name`)."""

    chat_name: str
    """Chat / conversation name the file arrived from."""

    sender: str = ""
    """Display name of the sender; empty string when unavailable."""

    file_name: str
    """Base file name including extension."""

    file_path: Path
    """Absolute path to the file on the local filesystem."""

    file_size: int
    """File size in bytes at the moment of detection."""

    created_at: datetime
    """UTC timestamp when the event was created."""

    metadata: dict[str, Any] = Field(default_factory=dict)
    """Arbitrary extra data attached by the source (e.g. message ID, thumb URL)."""


class TransferTask(BaseModel):
    """A persistent unit of work that tracks one file from detection to cleanup."""

    task_id: str
    """Unique identifier for this task (e.g. ``uuid4().hex``)."""

    file_hash: str
    """SHA-256 hex digest of the file contents (used for deduplication)."""

    source_name: str
    """Name of the originating source."""

    chat_name: str
    """Chat / conversation name the file arrived from."""

    file_name: str
    """Base file name including extension."""

    local_path: Path
    """Absolute path to the staged local copy."""

    remote_target: str
    """Name of the destination storage (matches :attr:`StorageConfig.name`)."""

    remote_path: str
    """Resolved destination path on the remote storage."""

    status: TaskStatus = TaskStatus.DETECTED
    """Current lifecycle status of this task."""

    retry_count: int = 0
    """Number of upload attempts that have already failed."""

    error_message: str | None = None
    """Last error message, or ``None`` when no error has occurred."""

    created_at: datetime
    """UTC timestamp when the task was created."""

    updated_at: datetime
    """UTC timestamp of the most recent status update."""
