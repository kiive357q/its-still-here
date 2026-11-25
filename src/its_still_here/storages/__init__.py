"""Storage providers for *its-still-here*.

Importable providers:
    - :class:`~its_still_here.storages.base.BaseStorageProvider`
    - :class:`~its_still_here.storages.rclone.RcloneProvider`
    - :class:`~its_still_here.storages.webdav.WebDAVProvider`
"""

from its_still_here.storages.base import BaseStorageProvider
from its_still_here.storages.rclone import RcloneProvider
from its_still_here.storages.webdav import WebDAVProvider

__all__ = [
    "BaseStorageProvider",
    "RcloneProvider",
    "WebDAVProvider",
]
