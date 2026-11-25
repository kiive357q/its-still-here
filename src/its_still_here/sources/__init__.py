"""Source adapters for *its-still-here*.

Importable adapters:
    - :class:`~its_still_here.sources.base.BaseSourceAdapter`
    - :class:`~its_still_here.sources.folder.WatchFolderAdapter`
    - :class:`~its_still_here.sources.wechat.WeChatAdapter`
"""

from its_still_here.sources.base import BaseSourceAdapter
from its_still_here.sources.folder import WatchFolderAdapter
from its_still_here.sources.wechat import WeChatAdapter

__all__ = [
    "BaseSourceAdapter",
    "WatchFolderAdapter",
    "WeChatAdapter",
]
