"""Folder-watcher source adapter using watchdog."""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from uuid import uuid4

from watchdog.events import FileCreatedEvent, FileSystemEventHandler
from watchdog.observers import Observer

from its_still_here.config import SourceConfig
from its_still_here.core.models import FileEvent
from its_still_here.sources.base import BaseSourceAdapter

logger = logging.getLogger(__name__)

# Suffixes that indicate the file is still being written / downloaded.
_TEMP_SUFFIXES: frozenset[str] = frozenset(
    {".tmp", ".crdownload", ".part", ".downloading"}
)


class _StableFileHandler(FileSystemEventHandler):
    """Watchdog event handler that waits for files to stabilise before emitting."""

    def __init__(
        self,
        watch_dir: Path,
        stabilize_seconds: float,
        callback: Callable[[FileEvent], None],
        source_name: str,
    ) -> None:
        super().__init__()
        self._watch_dir = watch_dir
        self._stabilize_seconds = stabilize_seconds
        self._callback = callback
        self._source_name = source_name

        # Maps absolute path → Timer thread that fires when the file is stable.
        self._pending: dict[str, threading.Timer] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # watchdog interface
    # ------------------------------------------------------------------

    def on_created(self, event: FileCreatedEvent) -> None:  # type: ignore[override]
        if event.is_directory:
            return
        self._schedule_check(event.src_path)

    def on_modified(self, event: FileCreatedEvent) -> None:  # type: ignore[override]
        if event.is_directory:
            return
        self._schedule_check(event.src_path)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _schedule_check(self, raw_path: str) -> None:
        path = Path(raw_path)
        if path.suffix.lower() in _TEMP_SUFFIXES:
            logger.debug("Ignoring temporary file: %s", path)
            return

        key = str(path)
        with self._lock:
            existing = self._pending.pop(key, None)
            if existing is not None:
                existing.cancel()

            timer = threading.Timer(
                self._stabilize_seconds, self._check_stable, args=(path,)
            )
            timer.daemon = True
            self._pending[key] = timer
            timer.start()

    def _check_stable(self, path: Path) -> None:
        """Called when no new events arrived within *stabilize_seconds*.

        Performs a final size-double-check to guard against false positives,
        then fires the callback.
        """
        key = str(path)
        with self._lock:
            self._pending.pop(key, None)

        if not path.exists():
            logger.debug("File disappeared before stabilisation: %s", path)
            return

        try:
            size_before = path.stat().st_size
            time.sleep(0.5)
            if not path.exists():
                return
            size_after = path.stat().st_size
        except OSError as exc:
            logger.warning("Cannot stat %s: %s", path, exc)
            return

        if size_before != size_after:
            # File is still growing – reschedule.
            self._schedule_check(str(path))
            return

        chat_name = self._extract_chat_name(path)
        event = FileEvent(
            event_id=uuid4().hex,
            source_name=self._source_name,
            chat_name=chat_name,
            sender="",
            file_name=path.name,
            file_path=path,
            file_size=size_after,
            created_at=datetime.now(timezone.utc),
            metadata={},
        )
        logger.info(
            "Stable file detected [source=%s chat=%s]: %s (%d bytes)",
            self._source_name,
            chat_name,
            path,
            size_after,
        )
        try:
            self._callback(event)
        except Exception:
            logger.exception("Error in FileEvent callback for %s", path)

    def _extract_chat_name(self, file_path: Path) -> str:
        """Return the immediate parent folder relative to *watch_dir*.

        If the file sits directly inside *watch_dir* (no sub-folder), returns
        ``"default"``.
        """
        try:
            rel = file_path.relative_to(self._watch_dir)
        except ValueError:
            return "default"

        parts = rel.parts
        # parts[-1] is the filename; parts[0] is the first sub-folder (if any).
        if len(parts) >= 2:
            return parts[0]
        return "default"

    def cancel_all(self) -> None:
        """Cancel all pending stability timers."""
        with self._lock:
            for timer in self._pending.values():
                timer.cancel()
            self._pending.clear()


class WatchFolderAdapter(BaseSourceAdapter):
    """Source adapter that watches a local directory for new / modified files.

    Uses :mod:`watchdog` to receive filesystem events and a background timer
    per file to wait for size stabilisation before emitting a
    :class:`~its_still_here.core.models.FileEvent`.

    Parameters
    ----------
    config:
        Must have ``type == "folder"`` and a valid ``watch_dir``.
    """

    def __init__(self, config: SourceConfig) -> None:
        super().__init__(config)
        self._observer: Observer | None = None
        self._handler: _StableFileHandler | None = None

    # ------------------------------------------------------------------
    # BaseSourceAdapter interface
    # ------------------------------------------------------------------

    def start(self, callback: Callable[[FileEvent], None]) -> None:
        """Start the watchdog observer.

        Parameters
        ----------
        callback:
            Invoked from a background thread for each stable
            :class:`FileEvent`.

        Raises
        ------
        ValueError
            If ``config.watch_dir`` is ``None`` or does not exist.
        """
        watch_dir = self.config.watch_dir
        if watch_dir is None:
            raise ValueError(
                f"Source '{self.config.name}': watch_dir must be set for type='folder'."
            )
        watch_dir = Path(watch_dir)
        if not watch_dir.exists():
            raise ValueError(
                f"Source '{self.config.name}': watch_dir '{watch_dir}' does not exist."
            )

        self._handler = _StableFileHandler(
            watch_dir=watch_dir,
            stabilize_seconds=self.config.stabilize_seconds,
            callback=callback,
            source_name=self.config.name,
        )
        self._observer = Observer()
        self._observer.schedule(self._handler, str(watch_dir), recursive=True)
        self._observer.start()
        logger.info(
            "WatchFolderAdapter [%s] started watching: %s", self.config.name, watch_dir
        )

    def stop(self) -> None:
        """Stop the watchdog observer and cancel pending timers."""
        if self._handler is not None:
            self._handler.cancel_all()
        if self._observer is not None:
            self._observer.stop()
            self._observer.join(timeout=10)
            self._observer = None
        logger.info("WatchFolderAdapter [%s] stopped.", self.config.name)

    def check_health(self) -> tuple[bool, str]:
        """Check that the watch directory exists and is readable.

        Returns
        -------
        tuple[bool, str]
            ``(True, "OK")`` or ``(False, <reason>)``.
        """
        watch_dir = self.config.watch_dir
        if watch_dir is None:
            return False, "watch_dir is not configured."
        watch_dir = Path(watch_dir)
        if not watch_dir.exists():
            return False, f"watch_dir '{watch_dir}' does not exist."
        if not watch_dir.is_dir():
            return False, f"watch_dir '{watch_dir}' is not a directory."
        return True, "OK"
