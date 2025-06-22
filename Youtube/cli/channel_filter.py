import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Set

logger = logging.getLogger(__name__)


class ChannelFilter:
    """
    Manages various filtering operations for channels based on different criteria.

    Provides methods to filter a dictionary of channel information using various
    conditions, such as requested channels, required and forbidden tags, activity
    dates, and more. The class also integrates with an external YouTube client
    to fetch necessary information for specific filters. Designed to streamline
    the process of narrowing down a list of channels to meet user-defined
    requirements.

    Attributes:
        channel_manager (ChannelManager): An instance responsible for managing
            channel-related operations and interactions.
    """

    def __init__(self, channel_manager):
        self.channel_manager = channel_manager

    # Helper utilities
    @staticmethod
    def _csv_to_set(csv: str) -> Set[str]:
        """Convert a comma-separated list into a lower-cased, trimmed `set`."""
        return {value.strip().lower() for value in csv.split(",") if value.strip()}

    @staticmethod
    def _matches_any_tag(channel_info: Dict, tags: Set[str]) -> bool:
        """Return True if the channel contains at least one tag from `tags`."""
        return any(tag.lower() in tags for tag in channel_info.get("tags", []))

    # Public API
    def apply_filters(self, channels: Dict, args, youtube_client) -> Dict:

        filtered = channels.copy()

        filtered = self._remove_outdated(filtered)
        if args.channels:
            filtered = self._keep_requested_channels(filtered, args.channels)
        if args.types:
            filtered = self._keep_required_tags(filtered, args.types)
        if args.limit_top:
            filtered = self._limit_to_top_n(filtered, youtube_client, args.limit_top)
        if args.skip:
            filtered = self._skip_channels(filtered, args.skip)
        if args.exclude_types:
            filtered = self._exclude_forbidden_tags(filtered, args.exclude_types)
        if args.max_inactive_days:
            filtered = self._keep_recently_active(
                filtered, youtube_client, args.max_inactive_days
            )

        return filtered

    # Individual filter helpers
    @staticmethod
    def _remove_outdated(channels: Dict) -> Dict:
        return {k: v for k, v in channels.items() if not v.get("outdated")}

    def _keep_requested_channels(self, channels: Dict, csv_names: str) -> Dict:
        requested = self._csv_to_set(csv_names)
        return {k: v for k, v in channels.items() if k.lower() in requested}

    def _keep_required_tags(self, channels: Dict, csv_tags: str) -> Dict:
        required_tags = self._csv_to_set(csv_tags)
        return {
            k: v for k, v in channels.items() if self._matches_any_tag(v, required_tags)
        }

    def _limit_to_top_n(
            self, channels: Dict, youtube_client, limit: int
    ) -> Dict:
        return dict(
            self.channel_manager.get_top_channels(youtube_client, channels, limit)
        )

    def _skip_channels(self, channels: Dict, csv_names: str) -> Dict:
        skip_set = self._csv_to_set(csv_names)
        return {k: v for k, v in channels.items() if k.lower() not in skip_set}

    def _exclude_forbidden_tags(self, channels: Dict, csv_tags: str) -> Dict:
        forbidden_tags = self._csv_to_set(csv_tags)
        return {
            k: v
            for k, v in channels.items()
            if not self._matches_any_tag(v, forbidden_tags)
        }

    def _keep_recently_active(
            self, channels: Dict, youtube_client, max_inactive_days: int
    ) -> Dict:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=max_inactive_days)
        still_active: Dict = {}
        for name, info in channels.items():
            last_dt = self.channel_manager.get_last_upload_date(
                youtube_client, info["channel_id"]
            )
            if last_dt and last_dt >= cutoff:
                still_active[name] = info
            else:
                logger.info(
                    "Skipping %s (last upload %s)",
                    name,
                    last_dt.date() if last_dt else "never",
                )
        return still_active
