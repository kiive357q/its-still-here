"""WeChat source adapter using wxauto / wxauto4."""

from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from uuid import uuid4

from its_still_here.config import SourceConfig
from its_still_here.core.models import FileEvent
from its_still_here.sources.base import BaseSourceAdapter

logger = logging.getLogger(__name__)

# Message types that carry downloadable files or videos.
_FILE_MSG_TYPES: frozenset[str] = frozenset({"file", "video", "File", "Video"})


class WeChatAdapter(BaseSourceAdapter):
    """Source adapter that listens to WeChat chats via *wxauto* or *wxauto4*.

    The correct library is selected lazily at :meth:`start` time based on
    ``config.wechat_version``:

    * ``"3.9"`` → ``import wxauto``
    * ``"4.0"`` → ``import wxauto4``

    File / video downloads are offloaded to a :class:`~concurrent.futures.ThreadPoolExecutor`
    so the wxauto listener loop is never blocked by large transfers.  After
    each download the adapter waits for the file size to stabilise (using
    ``config.stabilize_seconds``) before emitting a :class:`FileEvent`.

    Parameters
    ----------
    config:
        Must have ``type == "wechat"``.
    """

    def __init__(self, config: SourceConfig) -> None:
        super().__init__(config)
        self._wx = None  # wxauto WeChat instance
        self._executor: ThreadPoolExecutor | None = None
        self._running = False
        self._callback: Callable[[FileEvent], None] | None = None
        self._listener_thread = None

    # ------------------------------------------------------------------
    # BaseSourceAdapter interface
    # ------------------------------------------------------------------

    def start(self, callback: Callable[[FileEvent], None]) -> None:
        """Initialise wxauto and begin listening to configured chats.

        Parameters
        ----------
        callback:
            Called from worker threads each time a stable file is detected.

        Raises
        ------
        RuntimeError
            If the required wxauto library is not installed.
        """
        self._wx = self._load_wxauto()  # raises RuntimeError if unavailable
        self._callback = callback
        self._running = True
        self._executor = ThreadPoolExecutor(
            max_workers=4,
            thread_name_prefix=f"wechat-dl-{self.config.name}",
        )

        wx = self._wx
        for chat in self.config.listen_chats:
            try:
                wx.AddListenChat(who=chat, savepic=False)
                logger.info(
                    "WeChatAdapter [%s] listening to chat: %s", self.config.name, chat
                )
            except Exception as exc:
                logger.warning(
                    "WeChatAdapter [%s] failed to add chat '%s': %s",
                    self.config.name,
                    chat,
                    exc,
                )

        import threading

        self._listener_thread = threading.Thread(
            target=self._poll_loop,
            name=f"wechat-poll-{self.config.name}",
            daemon=True,
        )
        self._listener_thread.start()
        logger.info("WeChatAdapter [%s] started.", self.config.name)

    def stop(self) -> None:
        """Stop the polling loop and shut down the download executor."""
        self._running = False
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=False)
            self._executor = None
        logger.info("WeChatAdapter [%s] stopped.", self.config.name)

    def check_health(self) -> tuple[bool, str]:
        """Return ``(True, "OK")`` if the wxauto library is importable.

        Returns
        -------
        tuple[bool, str]
            Health status tuple.
        """
        try:
            self._load_wxauto()
        except RuntimeError as exc:
            return False, str(exc)
        return True, "OK"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_wxauto(self):
        """Import and return an initialised WeChat automation instance.

        Raises
        ------
        RuntimeError
            If neither ``wxauto`` nor ``wxauto4`` is importable for the
            configured ``wechat_version``.
        """
        version = self.config.wechat_version
        if version == "3.9":
            try:
                import wxauto  # type: ignore[import-untyped]

                return wxauto.WeChat()
            except ImportError:
                raise RuntimeError(
                    "wxauto is not installed. "
                    "Install it with: pip install wxauto"
                ) from None
        else:  # "4.0"
            try:
                import wxauto4  # type: ignore[import-untyped]

                return wxauto4.WeChat()
            except ImportError:
                raise RuntimeError(
                    "wxauto4 is not installed. "
                    "Install it with: pip install wxauto4"
                ) from None

    def _poll_loop(self) -> None:
        """Main polling loop; runs in a background daemon thread."""
        wx = self._wx
        while self._running:
            try:
                messages = wx.GetListenMessage()
                if messages:
                    for chat_name, msg_list in messages.items():
                        for msg in msg_list:
                            self._handle_message(chat_name, msg)
            except Exception as exc:
                logger.warning(
                    "WeChatAdapter [%s] poll error: %s", self.config.name, exc
                )
            time.sleep(0.5)

    def _handle_message(self, chat_name: str, msg) -> None:
        """Dispatch file/video messages to the download executor."""
        msg_type = getattr(msg, "type", None) or getattr(msg, "Type", None)
        if str(msg_type) not in _FILE_MSG_TYPES:
            return

        sender = getattr(msg, "sender", "") or getattr(msg, "Sender", "") or ""
        logger.debug(
            "WeChatAdapter [%s] queuing download for chat=%s sender=%s",
            self.config.name,
            chat_name,
            sender,
        )
        if self._executor is not None:
            self._executor.submit(
                self._download_and_emit, chat_name, str(sender), msg
            )

    def _download_and_emit(self, chat_name: str, sender: str, msg) -> None:
        """Download a file attachment and emit a :class:`FileEvent` when stable."""
        try:
            download_dir = self.config.download_dir
            if download_dir is not None:
                download_dir = Path(download_dir)
                download_dir.mkdir(parents=True, exist_ok=True)

            # Ask wxauto to save the file; pass savePath if download_dir given.
            save_kwargs: dict = {}
            if download_dir is not None:
                save_kwargs["savePath"] = str(download_dir)

            saved_path_raw = msg.Save(**save_kwargs) if hasattr(msg, "Save") else None
            if saved_path_raw is None:
                # Some wxauto versions expose .SaveFile or similar.
                saved_path_raw = (
                    msg.SaveFile(**save_kwargs)
                    if hasattr(msg, "SaveFile")
                    else None
                )

            if saved_path_raw is None:
                logger.warning(
                    "WeChatAdapter [%s] could not save message in chat=%s",
                    self.config.name,
                    chat_name,
                )
                return

            file_path = Path(str(saved_path_raw))
            if not file_path.exists():
                logger.warning(
                    "WeChatAdapter [%s] saved path does not exist: %s",
                    self.config.name,
                    file_path,
                )
                return

            # Wait for size stabilisation.
            self._wait_stable(file_path)

            if not file_path.exists():
                return

            file_size = file_path.stat().st_size
            event = FileEvent(
                event_id=uuid4().hex,
                source_name=self.config.name,
                chat_name=chat_name,
                sender=sender,
                file_name=file_path.name,
                file_path=file_path,
                file_size=file_size,
                created_at=datetime.now(timezone.utc),
                metadata={},
            )
            logger.info(
                "WeChatAdapter [%s] stable file [chat=%s sender=%s]: %s (%d bytes)",
                self.config.name,
                chat_name,
                sender,
                file_path,
                file_size,
            )
            if self._callback is not None:
                self._callback(event)
        except Exception:
            logger.exception(
                "WeChatAdapter [%s] error processing message in chat=%s",
                self.config.name,
                chat_name,
            )

    def _wait_stable(self, path: Path) -> None:
        """Block until *path* stops growing (size unchanged for stabilize_seconds)."""
        stabilize = self.config.stabilize_seconds
        poll_interval = min(1.0, stabilize / 4)
        last_size = -1

        while True:
            if not path.exists():
                return
            try:
                current_size = path.stat().st_size
            except OSError:
                return

            if current_size == last_size:
                # Size unchanged – wait the full stabilise window then recheck.
                time.sleep(stabilize)
                if not path.exists():
                    return
                try:
                    final_size = path.stat().st_size
                except OSError:
                    return
                if final_size == current_size:
                    return  # truly stable
                last_size = final_size
            else:
                last_size = current_size
                time.sleep(poll_interval)
