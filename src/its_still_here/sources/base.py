"""Abstract base class for all source adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable

from its_still_here.config import SourceConfig
from its_still_here.core.models import FileEvent


class BaseSourceAdapter(ABC):
    """Common interface that every file-source adapter must implement.

    Parameters
    ----------
    config:
        Validated :class:`~its_still_here.config.SourceConfig` for this source.
    """

    def __init__(self, config: SourceConfig) -> None:
        self.config = config

    @abstractmethod
    def start(self, callback: Callable[[FileEvent], None]) -> None:
        """Start the adapter and begin delivering :class:`FileEvent` objects.

        The *callback* will be invoked from a background thread each time a
        stable file is detected.  Implementations **must not** block the
        calling thread indefinitely; start internal threads / observers and
        return promptly.

        Parameters
        ----------
        callback:
            Callable that accepts a single :class:`FileEvent` and handles
            routing / persistence.  Must be thread-safe.
        """

    @abstractmethod
    def stop(self) -> None:
        """Stop the adapter and release all background resources gracefully."""

    @abstractmethod
    def check_health(self) -> tuple[bool, str]:
        """Return a ``(healthy, message)`` tuple.

        Returns
        -------
        tuple[bool, str]
            ``(True, "OK")`` when the adapter is ready to operate, or
            ``(False, <reason>)`` when it is not.
        """
