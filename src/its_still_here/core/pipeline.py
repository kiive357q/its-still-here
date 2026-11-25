"""Core synchronization pipeline orchestrating sources, routing, state, and storages."""

from __future__ import annotations

import hashlib
import logging
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from its_still_here.config import AppConfig
from its_still_here.core.db import StateDB
from its_still_here.core.models import FileEvent, TaskStatus, TransferTask
from its_still_here.core.router import RuleEngine
from its_still_here.sources import BaseSourceAdapter, WatchFolderAdapter, WeChatAdapter
from its_still_here.storages import BaseStorageProvider, RcloneProvider, WebDAVProvider

logger = logging.getLogger(__name__)

_HASH_CHUNK_SIZE = 1024 * 1024  # 1 MiB

_SOURCE_ADAPTERS: dict[str, type[BaseSourceAdapter]] = {
    "folder": WatchFolderAdapter,
    "wechat": WeChatAdapter,
}

_STORAGE_PROVIDERS: dict[str, type[BaseStorageProvider]] = {
    "rclone": RcloneProvider,
    "webdav": WebDAVProvider,
}


class SyncPipeline:
    """End-to-end file synchronization pipeline.

    Connects enabled :class:`BaseSourceAdapter` instances to enabled
    :class:`BaseStorageProvider` backends via :class:`RuleEngine` and persists
    all task transitions in :class:`StateDB`.
    """

    def __init__(self, config: AppConfig, max_workers: int = 4) -> None:
        self.config = config
        self._max_workers = max_workers
        self.db = StateDB(config.db_path)
        self.router = RuleEngine(config.rules)

        self.sources: dict[str, BaseSourceAdapter] = {}
        for src_cfg in config.sources:
            if not src_cfg.enabled:
                continue
            adapter_cls = _SOURCE_ADAPTERS.get(src_cfg.type)
            if adapter_cls is None:
                raise ValueError(f"Unsupported source type: {src_cfg.type!r}")
            self.sources[src_cfg.name] = adapter_cls(src_cfg)

        self.storages: dict[str, BaseStorageProvider] = {}
        for stg_cfg in config.storages:
            if not stg_cfg.enabled:
                continue
            provider_cls = _STORAGE_PROVIDERS.get(stg_cfg.type)
            if provider_cls is None:
                raise ValueError(f"Unsupported storage type: {stg_cfg.type!r}")
            self.storages[stg_cfg.name] = provider_cls(stg_cfg)

        self._stop_event = threading.Event()
        self._executor_lock = threading.Lock()
        self._executor: ThreadPoolExecutor | None = ThreadPoolExecutor(
            max_workers=self._max_workers,
            thread_name_prefix="ish-upload",
        )
        self._retry_thread: threading.Thread | None = None

    @staticmethod
    def compute_sha256(path: Path) -> str:
        """Compute the SHA-256 hex digest of *path* using 1 MiB chunked reads."""
        digest = hashlib.sha256()
        with path.open("rb") as fh:
            while True:
                chunk = fh.read(_HASH_CHUNK_SIZE)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    def _ensure_executor(self) -> ThreadPoolExecutor:
        with self._executor_lock:
            if self._executor is None:
                self._executor = ThreadPoolExecutor(
                    max_workers=self._max_workers,
                    thread_name_prefix="ish-upload",
                )
            return self._executor

    def handle_event(self, event: FileEvent) -> None:
        """Route a detected :class:`FileEvent`, stage matching files, and enqueue uploads."""
        matches = self.router.match(event)
        if not matches:
            logger.debug(
                "No matching rules for event %s (%s from %s/%s)",
                event.event_id,
                event.file_name,
                event.source_name,
                event.chat_name,
            )
            return

        try:
            file_hash = self.compute_sha256(event.file_path)
        except OSError as exc:
            logger.error(
                "Failed to hash source file %s for event %s: %s",
                event.file_path,
                event.event_id,
                exc,
            )
            return

        executor = self._ensure_executor()

        for rule, remote_path in matches:
            if self.db.exists_completed(file_hash, rule.target):
                logger.info(
                    "Skipping duplicate file %s (sha256=%s) for target %s",
                    event.file_name,
                    file_hash[:12],
                    rule.target,
                )
                continue

            if rule.target not in self.storages:
                logger.warning(
                    "Rule '%s' targets storage '%s', which is missing or disabled.",
                    rule.name,
                    rule.target,
                )
                continue

            task_id = uuid4().hex
            stage_dir = self.config.staging_dir / task_id
            stage_dir.mkdir(parents=True, exist_ok=True)
            staged_path = (stage_dir / event.file_name).resolve()

            try:
                shutil.copy2(event.file_path, staged_path)
            except OSError as exc:
                logger.error(
                    "Failed to stage %s to %s: %s",
                    event.file_path,
                    staged_path,
                    exc,
                )
                self._cleanup_empty_dir(stage_dir)
                continue

            now = datetime.now(timezone.utc)
            task = TransferTask(
                task_id=task_id,
                file_hash=file_hash,
                source_name=event.source_name,
                chat_name=event.chat_name,
                file_name=event.file_name,
                local_path=staged_path,
                remote_target=rule.target,
                remote_path=remote_path,
                status=TaskStatus.STAGED,
                retry_count=0,
                error_message=None,
                created_at=now,
                updated_at=now,
            )
            self.db.upsert_task(task)
            logger.info(
                "Enqueued task %s: %s -> %s:%s",
                task_id,
                staged_path,
                rule.target,
                remote_path,
            )
            executor.submit(self._process_task, task, rule.mode)

    def _process_task(self, task: TransferTask, mode: str) -> None:
        """Execute upload, verification, and optional staging cleanup for one task."""
        self.db.update_status(task.task_id, TaskStatus.UPLOADING)
        provider = self.storages.get(task.remote_target)

        try:
            if provider is None:
                raise RuntimeError(
                    f"Storage provider '{task.remote_target}' is not configured or enabled."
                )

            if not provider.upload(task.local_path, task.remote_path):
                raise RuntimeError(
                    f"Upload failed for '{task.file_name}' to '{task.remote_target}:{task.remote_path}'."
                )

            if not provider.verify(task.local_path, task.remote_path):
                raise RuntimeError(
                    f"Verification failed for '{task.file_name}' at '{task.remote_target}:{task.remote_path}'."
                )

            self.db.update_status(task.task_id, TaskStatus.VERIFIED)

            if mode == "move":
                task.local_path.unlink(missing_ok=True)
                self._cleanup_empty_dir(task.local_path.parent)
                self.db.update_status(task.task_id, TaskStatus.CLEANED)

        except Exception as exc:
            next_status = (
                TaskStatus.FAILED
                if (task.retry_count + 1) >= self.config.max_retries
                else TaskStatus.PENDING_RETRY
            )
            logger.warning(
                "Task %s failed (attempt %d/%d -> %s): %s",
                task.task_id,
                task.retry_count + 1,
                self.config.max_retries,
                next_status.value,
                exc,
            )
            self.db.update_status(
                task.task_id,
                next_status,
                error_message=str(exc),
                increment_retry=True,
            )

    def _resolve_mode(self, task: TransferTask) -> str:
        """Infer the transfer mode (``move`` or ``copy``) for a retried task."""
        for rule in self.config.rules:
            if not rule.enabled or rule.target != task.remote_target:
                continue
            if rule.sources and task.source_name not in rule.sources:
                continue
            if rule.include_chats and task.chat_name not in rule.include_chats:
                continue
            if task.chat_name in rule.exclude_chats:
                continue
            return rule.mode
        return "move"

    @staticmethod
    def _cleanup_empty_dir(directory: Path) -> None:
        try:
            if directory.exists() and directory.is_dir() and not any(directory.iterdir()):
                directory.rmdir()
        except OSError:
            logger.debug("Could not remove staging directory %s", directory, exc_info=True)

    def retry_pending_loop(self) -> None:
        """Periodically re-submit ``PENDING_RETRY`` tasks until stopped."""
        logger.info(
            "Retry loop started (interval=%ds, max_retries=%d).",
            self.config.retry_interval_seconds,
            self.config.max_retries,
        )
        while not self._stop_event.is_set():
            if self._stop_event.wait(timeout=self.config.retry_interval_seconds):
                break
            pending = self.db.list_pending_tasks(self.config.max_retries)
            if not pending:
                continue
            logger.info("Retrying %d pending task(s).", len(pending))
            executor = self._ensure_executor()
            for task in pending:
                if self._stop_event.is_set():
                    break
                if not task.local_path.exists():
                    logger.error(
                        "Staged file missing for pending task %s: %s",
                        task.task_id,
                        task.local_path,
                    )
                    self.db.update_status(
                        task.task_id,
                        TaskStatus.FAILED,
                        error_message=f"Staged file missing: {task.local_path}",
                    )
                    continue
                mode = self._resolve_mode(task)
                executor.submit(self._process_task, task, mode)
        logger.info("Retry loop stopped.")

    def start(self) -> None:
        """Start all enabled source adapters and the background retry loop."""
        self._stop_event.clear()
        self._ensure_executor()

        for name, adapter in self.sources.items():
            adapter.start(self.handle_event)
            logger.info("Started source adapter: %s", name)

        if self._retry_thread is None or not self._retry_thread.is_alive():
            self._retry_thread = threading.Thread(
                target=self.retry_pending_loop,
                name="ish-retry-loop",
                daemon=True,
            )
            self._retry_thread.start()

    def stop(self) -> None:
        """Gracefully stop sources, retry loop, upload workers, and database."""
        self._stop_event.set()

        for name, adapter in self.sources.items():
            try:
                adapter.stop()
            except Exception:
                logger.exception("Error stopping source adapter '%s'", name)

        if self._retry_thread is not None and self._retry_thread.is_alive():
            self._retry_thread.join(timeout=5.0)
            self._retry_thread = None

        with self._executor_lock:
            executor = self._executor
            self._executor = None
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=False)

        self.db.close()
        logger.info("SyncPipeline stopped.")
