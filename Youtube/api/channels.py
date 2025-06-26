# File: Youtube/api/channels.py

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from dateutil.parser import parse

logger = logging.getLogger(__name__)


class ChannelManager:
    """
    Handles channel-related operations, including fetching channel statistics,
    verifying channel activity, and retrieving upload dates.

    Attributes:
        metadata (MetadataManager): Instance for managing video metadata.
        comments (CommentManager): Instance for managing video comments.
    """

    def __init__(self, metadata_manager, comment_manager):
        """
        Initializes the ChannelManager with metadata and comment managers.

        Args:
            metadata_manager: An instance of MetadataManager for video metadata operations.
            comment_manager: An instance of CommentManager for comment-related operations.
        """
        self.metadata = metadata_manager
        self.comments = comment_manager

    @staticmethod
    def get_top_channels(youtube_client, channels: Dict, n: int = 3) -> List[Tuple[str, Dict]]:
        """
        Retrieves the top N channels by subscriber count.

        Args:
            youtube_client: The YouTube API client used to fetch channel statistics.
            channels (Dict): A dictionary of channel names and their information.
            n (int): The number of top channels to retrieve. Defaults to 3.

        Returns:
            List[Tuple[str, Dict]]: A list of tuples containing channel names and their information.
        """
        channel_map = {name: info for name, info in channels.items() if info.get("channel_id")}
        if not channel_map:
            return []

        ids = list({info["channel_id"] for info in channel_map.values()})
        id_chunks = [ids[i:i + 50] for i in range(0, len(ids), 50)]
        subs_map = {}

        for chunk in id_chunks:
            def _req(svc):
                """
                Constructs the API request for fetching channel statistics.

                Args:
                    svc: The YouTube API service instance.

                Returns:
                    The API request object.
                """
                return svc.channels().list(
                    part="statistics",
                    id=",".join(chunk),
                    maxResults=len(chunk)
                )
            resp, service = youtube_client.retry_request(_req)
            if resp and resp.get("items"):
                for item in resp["items"]:
                    cid = item["id"]
                    try:
                        subs_map[cid] = int(item.get("statistics", {}).get("subscriberCount", "0"))
                    except (ValueError, TypeError):
                        subs_map[cid] = 0

        for name, info in channel_map.items():
            info["subscriber_count"] = subs_map.get(info["channel_id"], 0)

        sorted_channels = sorted(
            channel_map.items(),
            key=lambda x: x[1].get("subscriber_count", 0),
            reverse=True
        )
        return sorted_channels[:n]

    @staticmethod
    def get_last_upload_date(youtube_client, channel_id: str) -> Optional[datetime]:
        """
        Retrieves the date of the latest upload for a channel.

        Args:
            youtube_client: The YouTube API client used to fetch channel details.
            channel_id (str): The ID of the channel to retrieve the upload date for.

        Returns:
            Optional[datetime]: The date of the latest upload, or None if unavailable.
        """
        def _chan_details(svc):
            """
            Constructs the API request for fetching channel details.

            Args:
                svc: The YouTube API service instance.

            Returns:
                The API request object.
            """
            return svc.channels().list(
                part="contentDetails",
                id=channel_id,
                maxResults=1,
            )
        resp, service = youtube_client.retry_request(_chan_details)
        if not resp or not resp.get("items"):
            return None

        uploads_pl = resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        def _pl_items(svc):
            """
            Constructs the API request for fetching playlist items.

            Args:
                svc: The YouTube API service instance.

            Returns:
                The API request object.
            """
            return svc.playlistItems().list(
                part="contentDetails",
                playlistId=uploads_pl,
                maxResults=1
            )
        resp, service = youtube_client.retry_request(_pl_items)
        if not resp or not resp.get("items"):
            return None

        last_date = resp["items"][0]["contentDetails"]["videoPublishedAt"]
        return parse(last_date).astimezone(timezone.utc)

    def verify_channels(self, youtube_client, channels: Dict[str, dict], cutoff_days: int = 365) -> Dict[str, dict]:
        """
        Verifies channel existence and activity based on the last upload date.

        Args:
            youtube_client: The YouTube API client used to verify channels.
            channels (Dict[str, dict]): A dictionary of channel names and their information.
            cutoff_days (int): The number of days to consider a channel inactive. Defaults to 365.

        Returns:
            Dict[str, dict]: A dictionary containing verification results for each channel.
        """
        cutoff_dt = datetime.now(tz=timezone.utc) - timedelta(days=cutoff_days)
        report = {}

        for name, info in channels.items():
            cid = info["channel_id"]

            def _exists(svc):
                """
                Constructs the API request for checking channel existence.

                Args:
                    svc: The YouTube API service instance.

                Returns:
                    The API request object.
                """
                return svc.channels().list(part="id", id=cid, maxResults=1)
            resp, service = youtube_client.retry_request(_exists)
            exists = bool(resp and resp.get("items"))
            if not exists:
                report[name] = {"exists": False, "last_upload": None, "inactive": True}
                continue

            last_dt = self.get_last_upload_date(youtube_client, cid)
            inactive = (last_dt is None) or (last_dt < cutoff_dt)
            report[name] = {
                "exists": True,
                "last_upload": last_dt,
                "inactive": inactive,
            }
        return report