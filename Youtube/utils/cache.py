# File: utils/cache.py
import atexit
import json
import os
from collections import OrderedDict
from datetime import datetime


class LRUCache(OrderedDict):
    """
    Implements a Least Recently Used (LRU) cache using an Ordered Dictionary.

    This class inherits from `OrderedDict` and provides functionality to store
    a fixed number of items. When the maximum size is reached, the least recently
    used item is automatically removed upon adding a new item. It is particularly
    useful for managing resources or data that have limited capacity.

    Attributes:
        max_size (int): Maximum number of items the cache can hold.
    """

    def __init__(self, max_size: int):
        self.max_size = max_size
        super().__init__()

    def __setitem__(self, key, value):
        if len(self) >= self.max_size:
            self.popitem(last=False)
        super().__setitem__(key, value)


class CacheManager:
    """
    Manages caching for video and channel metadata.

    This class provides mechanisms to manage metadata caching for videos and
    channels using an LRU (Least Recently Used) cache. It supports persistent
    storage by saving and loading cache data to and from JSON files, ensuring
    data consistency between application runs.

    Attributes:
        max_cache_size (int): Maximum number of items that can be stored in the
            LRU cache.
        cache_dir (str): Directory path where cache files are stored.
        video_cache_file (str): Path to the JSON file storing video metadata
            cache.
        channel_cache_file (str): Path to the JSON file storing channel metadata
            cache.
        video_cache (LRUCache): In-memory cache for video metadata.
        channel_cache (LRUCache): In-memory cache for channel metadata.
    """

    def __init__(self, max_cache_size: int = 1000, cache_dir: str = "yt_cache"):
        self.max_cache_size = max_cache_size
        self.cache_dir = cache_dir
        self.video_cache_file = os.path.join(cache_dir, "video_metadata_cache.json")
        self.channel_cache_file = os.path.join(cache_dir, "channel_metadata_cache.json")

        self.video_cache: LRUCache = LRUCache(max_cache_size)
        self.channel_cache: LRUCache = LRUCache(max_cache_size)

        self._load_caches()
        atexit.register(self._save_caches)

    def _load_caches(self) -> None:
        """Load existing cache data from files."""
        if os.path.exists(self.video_cache_file):
            with open(self.video_cache_file, "r") as f:
                self.video_cache.update(json.load(f))
        if os.path.exists(self.channel_cache_file):
            with open(self.channel_cache_file, "r") as f:
                self.channel_cache.update(json.load(f))

    def _save_caches(self) -> None:
        """Save cache data to JSON files."""

        def _ser(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Cannot JSON-serialise {type(obj)}")

        os.makedirs(os.path.dirname(self.video_cache_file), exist_ok=True)
        with open(self.video_cache_file, "w") as vf:
            json.dump(dict(self.video_cache), vf, default=_ser)

        os.makedirs(os.path.dirname(self.channel_cache_file), exist_ok=True)
        with open(self.channel_cache_file, "w") as cf:
            json.dump(dict(self.channel_cache), cf, default=_ser)
