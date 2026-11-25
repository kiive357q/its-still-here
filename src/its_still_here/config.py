"""Configuration models and YAML loader for *its-still-here*."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field


class ProxyConfig(BaseModel):
    """Optional proxy used by a storage backend."""

    enabled: bool = False
    url: str | None = None


class SourceConfig(BaseModel):
    """Configuration for a single file source (folder watcher or WeChat)."""

    name: str
    """Unique logical name used to reference this source in rules."""

    type: Literal["folder", "wechat"]
    """Source driver type."""

    enabled: bool = True

    # --- folder-specific ---
    watch_dir: Path | None = None
    """Directory to watch when ``type == "folder"``."""

    stabilize_seconds: float = 2.0
    """Seconds of inactivity before a file is considered fully written."""

    # --- wechat-specific ---
    listen_chats: list[str] = Field(default_factory=list)
    """Chat names (or ``["*"]`` for all) to monitor when ``type == "wechat"``."""

    wechat_version: Literal["3.9", "4.0"] = "4.0"
    """WeChat Windows client version targeted by wxauto."""

    download_dir: Path | None = None
    """Where wxauto saves received files; falls back to wxauto default when ``None``."""


class StorageConfig(BaseModel):
    """Configuration for a single remote storage backend."""

    name: str
    """Unique logical name used to reference this storage in rules."""

    type: Literal["rclone", "webdav"]
    """Storage driver type."""

    enabled: bool = True

    # --- rclone ---
    remote_name: str | None = None
    """Rclone remote name (e.g. ``"myonedrive"``) when ``type == "rclone"``."""

    # --- webdav ---
    base_url: str | None = None
    username: str | None = None
    password: str | None = None

    # --- shared upload options ---
    root_path: str = "ItIsStillHere"
    """Root directory on the remote under which all files are placed."""

    chunk_size: str = "64M"
    """Upload chunk size passed to rclone (e.g. ``"64M"``)."""

    retries: int = 10
    """Number of rclone / httpx retries per upload attempt."""

    tpslimit: int = 8
    """Transactions-per-second cap passed to rclone."""

    proxy: ProxyConfig = Field(default_factory=ProxyConfig)
    """Proxy settings applied to this storage only."""


class RuleConfig(BaseModel):
    """A routing rule that maps matching file events to a storage target."""

    name: str
    """Human-readable rule name (must be unique for diagnostics)."""

    enabled: bool = True

    # --- source / chat filters ---
    sources: list[str] = Field(default_factory=list)
    """Source names this rule applies to; empty list means *all* sources."""

    include_chats: list[str] = Field(default_factory=list)
    """Chats to include; empty list means *all*."""

    exclude_chats: list[str] = Field(default_factory=list)
    """Chats to explicitly exclude (applied after ``include_chats``)."""

    # --- file filters ---
    include_exts: list[str] = Field(default_factory=list)
    """Extensions to include (e.g. ``[".pdf", ".docx"]``); empty means *all*."""

    exclude_exts: list[str] = Field(default_factory=list)
    """Extensions to exclude."""

    min_size_mb: float = 0.0
    """Minimum file size in megabytes (inclusive)."""

    max_size_mb: float | None = None
    """Maximum file size in megabytes (inclusive); ``None`` means unlimited."""

    # --- destination ---
    target: str
    """Storage name (matches :attr:`StorageConfig.name`) to upload to."""

    path_template: str = "{source}/{chat_name}/{YYYY}-{MM}/{filename}"
    """Template for the remote sub-path.

    Supported placeholders: ``{source}``, ``{chat_name}``, ``{sender}``,
    ``{filename}``, ``{stem}``, ``{ext}``, ``{YYYY}``, ``{MM}``, ``{DD}``,
    ``{HH}``, ``{mm}``, ``{ss}``.
    """

    mode: Literal["move", "copy"] = "move"
    """Whether to delete the local file after a successful upload."""


class AppConfig(BaseModel):
    """Top-level application configuration."""

    staging_dir: Path = Path("./staging")
    """Local directory used to stage files before upload."""

    db_path: Path = Path("./data/state.db")
    """SQLite database file path for persistent task state."""

    retry_interval_seconds: int = 300
    """Seconds between retry sweeps for :attr:`~TaskStatus.PENDING_RETRY` tasks."""

    max_retries: int = 15
    """Maximum number of upload attempts before a task is marked :attr:`~TaskStatus.FAILED`."""

    sources: list[SourceConfig] = Field(default_factory=list)
    storages: list[StorageConfig] = Field(default_factory=list)
    rules: list[RuleConfig] = Field(default_factory=list)


def load_config(path: Path) -> AppConfig:
    """Load and validate an :class:`AppConfig` from a YAML file.

    Args:
        path: Filesystem path to the YAML configuration file.

    Returns:
        A fully-validated :class:`AppConfig` instance.

    Raises:
        FileNotFoundError: If *path* does not exist.
        yaml.YAMLError: If the file cannot be parsed as YAML.
        pydantic.ValidationError: If the parsed data fails schema validation.
    """
    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    return AppConfig.model_validate(raw)
