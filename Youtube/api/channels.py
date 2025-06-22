# File: api/channels.py

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from dateutil.parser import parse

logger = logging.getLogger(__name__)


class ChannelManager:
    """Handles channel-related operations."""

    def __init__(self, metadata_manager, comment_manager):
        self.metadata = metadata_manager
        self.comments = comment_manager

    @staticmethod
    def get_top_channels(youtube_client, channels: Dict, n: int = 3) -> List[Tuple[str, Dict]]:
        """Get top N channels by subscriber count."""
        for name, info in channels.items():
            if info.get("channel_id"):
                def _req(svc):
                    return svc.channels().list(
                        part="statistics",
                        id=info["channel_id"],
                        maxResults=1,
                    )
            elif info.get("handle"):
                def _req(svc):
                    return svc.channels().list(
                        part="statistics",
                        forHandle=info["handle"].lstrip("@"),
                        maxResults=1,
                    )
            else:
                logger.warning("Channel %s has neither channel_id nor handle", name)
                info["subscriber_count"] = 0
                continue

            resp, service = youtube_client.retry_request(_req)
            try:
                subs = int(
                    resp.get("items", [{}])[0]
                    .get("statistics", {})
                    .get("subscriberCount", "0")
                )
            except (AttributeError, IndexError, ValueError, TypeError):
                logger.warning("Could not fetch subscriber count for %s", name)
                subs = 0

            info["subscriber_count"] = subs

        return sorted(
            channels.items(),
            key=lambda x: x[1].get("subscriber_count", 0),
            reverse=True
        )[:n]

    @staticmethod
    def get_last_upload_date(youtube_client, channel_id: str) -> Optional[datetime]:
        """Get the date of the latest upload for a channel."""

        def _chan_details(svc):
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

    def verify_channels(
            self,
            youtube_client,
            channels: Dict[str, dict],
            cutoff_days: int = 365
    ) -> Dict[str, dict]:
        """Verify channel existence and activity."""
        cutoff_dt = datetime.now(tz=timezone.utc) - timedelta(days=cutoff_days)
        report = {}

        for name, info in channels.items():
            cid = info["channel_id"]

            def _exists(svc):
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
