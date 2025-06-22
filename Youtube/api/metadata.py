# File: api/metadata.py
import logging
from datetime import datetime
from typing import Dict, Optional, Sequence, Tuple

from dateutil.parser import parse

from Youtube.utils.cache import CacheManager

logger = logging.getLogger(__name__)


class MetadataManager:
    """
    Manages and provides metadata for YouTube videos.

    This class interfaces with both a cache and the YouTube API to retrieve and cache
    video metadata. Metadata retrieval includes fetching video titles, publication
    dates, channel IDs, and associated channel names. The class supports both
    batch and single-video retrieval, optimizing for cached data to minimize API
    calls.

    Attributes:
        cache (CacheManager): Object responsible for managing cached video and
            channel data.
    """

    def __init__(self, cache_manager: CacheManager):
        self.cache = cache_manager

    def batch_fetch_video_metadata(
            self,
            youtube_client,
            video_ids: Sequence[str],
    ) -> Dict[str, Tuple[str, Optional[datetime], str]]:
        """
        Fetches metadata for a batch of YouTube videos.

        Returns:
            Dict mapping video_id to (title, publish_date, channel_id)
        """
        if not video_ids:
            return {}

        missing_ids = [vid for vid in video_ids if vid not in self.cache.video_cache]

        if missing_ids:
            def _videos_request(svc):
                return svc.videos().list(
                    part="snippet",
                    id=",".join(missing_ids),
                    maxResults=len(missing_ids),
                    fields="items(id,snippet(channelId,title,publishedAt))",
                )

            response, service = youtube_client.retry_request(_videos_request)
            if response:
                for item in response.get("items", []):
                    video_id = item["id"]
                    snippet = item["snippet"]
                    channel_id = snippet["channelId"]
                    owner_name = self.cache.channel_cache.get(channel_id)

                    self.cache.video_cache[video_id] = (
                        snippet["title"],
                        owner_name,
                        parse(snippet["publishedAt"]) if snippet.get("publishedAt") else None,
                        channel_id,
                    )

        return {
            vid: (cached[0], cached[2], cached[3])
            for vid in video_ids
            if (cached := self.cache.video_cache.get(vid)) is not None
        }

    def fetch_video_metadata(
            self,
            youtube_client,
            video_id: str,
    ) -> Tuple[Optional[str], Optional[str], Optional[datetime], Optional[str]]:
        """
        Fetches single video metadata.

        Returns:
            Tuple of (title, owner_name, publish_date, owner_id)
        """
        if video_id in self.cache.video_cache:
            return self.cache.video_cache[video_id]

        def _req(svc):
            return svc.videos().list(
                part="snippet",
                id=video_id,
                maxResults=1,
                fields="items(snippet(channelId,title,publishedAt))",
            )

        resp, service = youtube_client.retry_request(_req)
        if not resp or not resp.get("items"):
            return None, None, None, None

        snip = resp["items"][0]["snippet"]
        owner_id = snip["channelId"]
        title = snip["title"]
        publish_dt = parse(snip["publishedAt"]) if snip.get("publishedAt") else None
        owner_name = self.cache.channel_cache.get(owner_id)

        self.cache.video_cache[video_id] = (title, owner_name, publish_dt, owner_id)
        return title, owner_name, publish_dt, owner_id
