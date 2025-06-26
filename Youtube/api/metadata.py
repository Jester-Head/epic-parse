# File: api/metadata.py
import logging
from datetime import datetime
from typing import Dict, Optional, Sequence, Tuple

from dateutil.parser import parse

from utils.cache import CacheManager

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
        """
        Initializes the MetadataManager with a cache manager.

        Args:
            cache_manager (CacheManager): The cache manager instance to handle
                cached video and channel data.
        """
        self.cache = cache_manager

    def batch_fetch_video_metadata(
            self,
            youtube_client,
            video_ids: Sequence[str],
    ) -> Dict[str, Tuple[str, Optional[datetime], str]]:
        """
        Fetches metadata for a batch of YouTube videos.

        Args:
            youtube_client: The YouTube API client used to fetch metadata.
            video_ids (Sequence[str]): A list of video IDs to fetch metadata for.

        Returns:
            Dict[str, Tuple[str, Optional[datetime], str]]: A dictionary mapping
            video IDs to tuples containing the video title, publish date, and
            channel ID.
        """
        if not video_ids:
            return {}

        # Filter out video IDs that are already cached
        missing_ids = [vid for vid in video_ids if vid not in self.cache.video_cache]

        if missing_ids:
            def _videos_request(svc):
                """
                Constructs the API request for fetching video metadata.

                Args:
                    svc: The YouTube API service instance.

                Returns:
                    The API request object.
                """
                return svc.videos().list(
                    part="snippet",
                    id=",".join(missing_ids),
                    maxResults=len(missing_ids),
                    fields="items(id,snippet(channelId,title,publishedAt))",
                )

            # Fetch metadata for missing video IDs
            response, service = youtube_client.retry_request(_videos_request)
            if response:
                for item in response.get("items", []):
                    video_id = item["id"]
                    snippet = item["snippet"]
                    channel_id = snippet["channelId"]
                    owner_name = self.cache.channel_cache.get(channel_id)

                    # Cache the fetched metadata
                    self.cache.video_cache[video_id] = (
                        snippet["title"],
                        owner_name,
                        parse(snippet["publishedAt"]) if snippet.get("publishedAt") else None,
                        channel_id,
                    )

        # Return metadata for all requested video IDs, including cached ones
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
        Fetches metadata for a single YouTube video.

        Args:
            youtube_client: The YouTube API client used to fetch metadata.
            video_id (str): The ID of the video to fetch metadata for.

        Returns:
            Tuple[Optional[str], Optional[str], Optional[datetime], Optional[str]]:
            A tuple containing the video title, channel name, publish date, and
            channel ID. Returns None for any field if metadata is unavailable.
        """
        logger.info("Fetching metadata for video ID: %s", video_id)

        # Check if metadata is already cached
        if video_id in self.cache.video_cache:
            logger.info("Cache hit for video ID: %s", video_id)
            return self.cache.video_cache[video_id]

        logger.info("Cache miss for video ID: %s. Fetching from API...", video_id)

        def _req(svc):
            """
            Constructs the API request for fetching video metadata.

            Args:
                svc: The YouTube API service instance.

            Returns:
                The API request object.
            """
            return svc.videos().list(
                part="snippet",
                id=video_id,
                maxResults=1,
                fields="items(snippet(channelId,title,publishedAt))",
            )

        try:
            # Fetch metadata from the YouTube API
            resp, service = youtube_client.retry_request(_req)
            if not resp or not resp.get("items"):
                logger.warning("No metadata found for video ID: %s", video_id)
                return None, None, None, None

            snip = resp["items"][0]["snippet"]
            owner_id = snip["channelId"]
            title = snip["title"]
            publish_dt = parse(snip["publishedAt"]) if snip.get("publishedAt") else None
            owner_name = self.cache.channel_cache.get(owner_id)

            # Cache the fetched metadata
            self.cache.video_cache[video_id] = (title, owner_name, publish_dt, owner_id)
            logger.info("Successfully fetched metadata for video ID: %s", video_id)
            return title, owner_name, publish_dt, owner_id

        except Exception as e:
            # Log any errors encountered during metadata fetching
            logger.error("Error fetching metadata for video ID: %s. Exception: %s", video_id, e, exc_info=True)
            return None, None, None, None