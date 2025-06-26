# File: Youtube/utils/cache.py

import atexit
import json
import os
from collections import OrderedDict
from datetime import datetime


class LRUCache(OrderedDict):
    """
    Implements a Least Recently Used (LRU) cache using an Ordered Dictionary.

    This class extends `OrderedDict` to provide an LRU caching mechanism, where
    the least recently used items are removed when the cache exceeds its maximum size.

    Attributes:
        max_size (int): The maximum number of items the cache can hold.
    """

    def __init__(self, max_size: int):
        """
        Initializes the LRUCache with a maximum size.

        Args:
            max_size (int): The maximum number of items the cache can hold.
        """
        self.max_size = max_size
        super().__init__()

    def __setitem__(self, key, value):
        """
        Adds an item to the cache. Removes the least recently used item if the cache exceeds its maximum size.

        Args:
            key: The key of the item to add.
            value: The value of the item to add.
        """
        if len(self) >= self.max_size:
            self.popitem(last=False)
        super().__setitem__(key, value)


class CacheManager:
    """
    Manages caching for video and channel metadata.

    This class provides methods to load, save, and manage cached data for YouTube video
    metadata, channel metadata, and etags. It uses `LRUCache` for efficient caching and
    persists cache data to JSON files.
    """

    def __init__(self, max_cache_size: int = 1000, cache_dir: str = "Youtube/yt_cache"):
        """
        Initializes the CacheManager with cache size and directory.

        Args:
            max_cache_size (int): The maximum number of items each cache can hold. Defaults to 1000.
            cache_dir (str): The directory where cache files are stored. Defaults to "yt_cache".
        """
        self.max_cache_size = max_cache_size
        self.cache_dir = cache_dir
        self.video_cache_file = os.path.join(cache_dir, "video_metadata_cache.json")
        self.etag_cache_file = os.path.join(cache_dir, "etag_cache.json")
        self.channel_cache = {}
        self.video_cache: LRUCache = LRUCache(max_cache_size)
        self.etag_cache: LRUCache = LRUCache(max_cache_size)

        self._load_caches()
        atexit.register(self._save_caches)

    def _load_caches(self) -> None:
        """
        Loads existing cache data from files into memory.

        This method reads cache files for video metadata and etags, and updates the
        respective caches with the loaded data.
        """
        if os.path.exists(self.video_cache_file):
            with open(self.video_cache_file, "r") as f:
                self.video_cache.update(json.load(f))
        if os.path.exists(self.etag_cache_file):
            with open(self.etag_cache_file, "r") as f:
                self.etag_cache.update(json.load(f))

    def _save_caches(self) -> None:
        """
        Saves cache data to JSON files.

        This method serializes the current state of the video metadata and etag caches
        to JSON files. It ensures that datetime objects are properly serialized.
        """

        def _ser(obj):
            """
            Custom serializer for JSON serialization.

            Args:
                obj: The object to serialize.

            Returns:
                str: The ISO format string for datetime objects.

            Raises:
                TypeError: If the object type is not supported for serialization.
            """
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Cannot JSON-serialise {type(obj)}")

        os.makedirs(os.path.dirname(self.video_cache_file), exist_ok=True)
        with open(self.video_cache_file, "w") as vf:
            json.dump(dict(self.video_cache), vf, default=_ser)

        os.makedirs(os.path.dirname(self.etag_cache_file), exist_ok=True)
        with open(self.etag_cache_file, "w") as ef:
            json.dump(dict(self.etag_cache), ef, default=_ser)