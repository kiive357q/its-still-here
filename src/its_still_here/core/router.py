"""Rule-based routing engine that matches :class:`FileEvent` to storage targets."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

from ..config import RuleConfig
from .models import FileEvent

# Characters that are unsafe in path segments (path traversal, shell injection).
_UNSAFE_RE = re.compile(r'[/\\<>:"|?*\x00-\x1f]')
_DOT_SEGMENT_RE = re.compile(r"^\.\.?$")

_MB = 1024 * 1024


def _normalize_ext(ext: str) -> str:
    """Return *ext* lowercased with a leading dot.

    >>> _normalize_ext("PDF")
    '.pdf'
    >>> _normalize_ext(".Pdf")
    '.pdf'
    """
    ext = ext.lower()
    return ext if ext.startswith(".") else f".{ext}"


def _sanitize_segment(value: str) -> str:
    """Remove path-traversal and shell-unsafe characters from a path segment.

    Args:
        value: Raw user-supplied string (chat name, sender name, etc.).

    Returns:
        A safe string suitable for use as a single path component.
    """
    cleaned = _UNSAFE_RE.sub("_", value).strip()
    cleaned = cleaned.replace("..", "_")
    # Collapse consecutive underscores / spaces, and reject dot-only segments.
    if _DOT_SEGMENT_RE.match(cleaned) or cleaned == "":
        cleaned = "_"
    return cleaned


def _render_path(template: str, rule: RuleConfig, event: FileEvent) -> str:
    """Expand *template* with event and time placeholders.

    Supported placeholders
    ----------------------
    ``{source}``      – :attr:`FileEvent.source_name`
    ``{chat_name}``   – :attr:`FileEvent.chat_name` (sanitized)
    ``{sender}``      – :attr:`FileEvent.sender` (sanitized)
    ``{filename}``    – :attr:`FileEvent.file_name`
    ``{stem}``        – filename without extension
    ``{ext}``         – extension without leading dot
    ``{YYYY}``        – 4-digit UTC year
    ``{MM}``          – 2-digit UTC month
    ``{DD}``          – 2-digit UTC day
    ``{HH}``          – 2-digit UTC hour
    ``{mm}``          – 2-digit UTC minute
    ``{ss}``          – 2-digit UTC second

    Args:
        template: Path template string from :attr:`RuleConfig.path_template`.
        rule: The matched rule (unused beyond providing the template).
        event: The file event being routed.

    Returns:
        A POSIX-style relative path string with no leading slash.
    """
    now: datetime = event.created_at.astimezone(timezone.utc)
    p = Path(event.file_name)
    stem = p.stem
    ext = p.suffix.lstrip(".")

    placeholders: dict[str, str] = {
        "source": _sanitize_segment(event.source_name),
        "chat_name": _sanitize_segment(event.chat_name),
        "sender": _sanitize_segment(event.sender) if event.sender else "_",
        "filename": event.file_name,
        "stem": stem,
        "ext": ext,
        "YYYY": now.strftime("%Y"),
        "MM": now.strftime("%m"),
        "DD": now.strftime("%d"),
        "HH": now.strftime("%H"),
        "mm": now.strftime("%M"),
        "ss": now.strftime("%S"),
    }
    rendered = template.format_map(placeholders)
    # Normalise to a clean POSIX path with no leading slash.
    return str(PurePosixPath(rendered))


class RuleEngine:
    """Matches a :class:`FileEvent` against an ordered list of routing rules.

    Args:
        rules: All configured :class:`RuleConfig` objects (enabled or not).
    """

    def __init__(self, rules: list[RuleConfig]) -> None:
        self._rules = rules

    def match(self, event: FileEvent) -> list[tuple[RuleConfig, str]]:
        """Return every rule that matches *event*, with its rendered remote path.

        Rules are evaluated in declaration order.  For each matching rule the
        rendered :attr:`RuleConfig.path_template` is returned alongside it.

        Matching criteria (all must pass):

        1. Rule is **enabled**.
        2. **sources** – empty list accepts all; otherwise ``event.source_name``
           must appear in the list.
        3. **include_chats** – empty list accepts all; otherwise
           ``event.chat_name`` must be in the list.
        4. **exclude_chats** – ``event.chat_name`` must *not* be in the list.
        5. **include_exts** – empty list accepts all; otherwise the file
           extension (case-insensitive, normalised with a leading dot) must
           appear in the list.
        6. **exclude_exts** – the extension must *not* be in the list.
        7. **min_size_mb** / **max_size_mb** – file size must be within bounds.

        Args:
            event: The :class:`FileEvent` to route.

        Returns:
            Ordered list of ``(rule, remote_path)`` tuples for all matching rules.
            Empty when no rule matches.
        """
        results: list[tuple[RuleConfig, str]] = []
        file_ext = _normalize_ext(Path(event.file_name).suffix)
        file_size_mb = event.file_size / _MB

        for rule in self._rules:
            if not rule.enabled:
                continue

            # 1. Source filter
            if rule.sources and event.source_name not in rule.sources:
                continue

            # 2. include_chats
            if rule.include_chats and event.chat_name not in rule.include_chats:
                continue

            # 3. exclude_chats
            if event.chat_name in rule.exclude_chats:
                continue

            # 4. include_exts (case-insensitive, normalised)
            if rule.include_exts:
                normalised = [_normalize_ext(e) for e in rule.include_exts]
                if file_ext not in normalised:
                    continue

            # 5. exclude_exts
            if rule.exclude_exts:
                normalised_ex = [_normalize_ext(e) for e in rule.exclude_exts]
                if file_ext in normalised_ex:
                    continue

            # 6. Size bounds
            if file_size_mb < rule.min_size_mb:
                continue
            if rule.max_size_mb is not None and file_size_mb > rule.max_size_mb:
                continue

            remote_path = _render_path(rule.path_template, rule, event)
            results.append((rule, remote_path))

        return results
