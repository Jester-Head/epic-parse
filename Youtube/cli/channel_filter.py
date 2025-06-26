# File: Youtube/cli/channel_filter.py

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Set

logger = logging.getLogger(__name__)


class ChannelFilter:
    """
    Manages various filtering operations for YouTube channels.

    This class provides methods to filter channels based on criteria such as tags,
    activity status, subscriber count, and inclusion/exclusion lists. It interacts
    with a `ChannelManager` instance to retrieve channel-related data.
    """

    def __init__(self, channel_manager):
        """
        Initializes the ChannelFilter with a ChannelManager instance.

        Args:
            channel_manager: An instance of ChannelManager to manage channel-related operations.
        """
        self.channel_manager = channel_manager

    @staticmethod
    def _csv_to_set(csv: str) -> Set[str]:
        """
        Converts a comma-separated string into a set of lower-cased, trimmed values.

        Args:
            csv (str): A comma-separated string.

        Returns:
            Set[str]: A set of trimmed, lower-cased values.
        """
        return {value.strip().lower() for value in csv.split(",") if value.strip()}

    @staticmethod
    def _matches_any_tag(channel_info: Dict, tags: Set[str]) -> bool:
        """
        Checks if a channel contains at least one tag from the given set of tags.

        Args:
            channel_info (Dict): A dictionary containing channel information.
            tags (Set[str]): A set of tags to match against.

        Returns:
            bool: True if the channel contains at least one matching tag, False otherwise.
        """
        return any(tag.lower() in tags for tag in channel_info.get("tags", []))

    def apply_filters(self, channels: Dict, args, youtube_client) -> Dict:
        """
        Applies a series of filters to the given channels based on the provided arguments.

        Args:
            channels (Dict): A dictionary of channel names and their information.
            args: Parsed command-line arguments containing filter criteria.
            youtube_client: The YouTube API client used for channel-related operations.

        Returns:
            Dict: A dictionary of filtered channels.
        """
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
            filtered = self._keep_recently_active(filtered, youtube_client, args.max_inactive_days)
        return filtered

    @staticmethod
    def _remove_outdated(channels: Dict) -> Dict:
        """
        Removes channels marked as outdated.

        Args:
            channels (Dict): A dictionary of channel names and their information.

        Returns:
            Dict: A dictionary of channels excluding those marked as outdated.
        """
        return {k: v for k, v in channels.items() if not v.get("outdated")}

    def _keep_requested_channels(self, channels: Dict, csv_names: str) -> Dict:
        """
        Filters channels to keep only those explicitly requested.

        Args:
            channels (Dict): A dictionary of channel names and their information.
            csv_names (str): A comma-separated list of requested channel names.

        Returns:
            Dict: A dictionary of channels matching the requested names.
        """
        requested = self._csv_to_set(csv_names)
        return {k: v for k, v in channels.items() if k.lower() in requested}

    def _keep_required_tags(self, channels: Dict, csv_tags: str) -> Dict:
        """
        Filters channels to keep only those containing required tags.

        Args:
            channels (Dict): A dictionary of channel names and their information.
            csv_tags (str): A comma-separated list of required tags.

        Returns:
            Dict: A dictionary of channels matching the required tags.
        """
        required_tags = self._csv_to_set(csv_tags)
        return {k: v for k, v in channels.items() if self._matches_any_tag(v, required_tags)}

    def _limit_to_top_n(self, channels: Dict, youtube_client, limit: int) -> Dict:
        """
        Limits the channels to the top N by subscriber count.

        Args:
            channels (Dict): A dictionary of channel names and their information.
            youtube_client: The YouTube API client used to fetch subscriber counts.
            limit (int): The maximum number of channels to keep.

        Returns:
            Dict: A dictionary of the top N channels by subscriber count.
        """
        return dict(self.channel_manager.get_top_channels(youtube_client, channels, limit))

    def _skip_channels(self, channels: Dict, csv_names: str) -> Dict:
        """
        Filters channels to exclude those explicitly listed.

        Args:
            channels (Dict): A dictionary of channel names and their information.
            csv_names (str): A comma-separated list of channel names to exclude.

        Returns:
            Dict: A dictionary of channels excluding the listed names.
        """
        skip_set = self._csv_to_set(csv_names)
        return {k: v for k, v in channels.items() if k.lower() not in skip_set}

    def _exclude_forbidden_tags(self, channels: Dict, csv_tags: str) -> Dict:
        """
        Filters channels to exclude those containing forbidden tags.

        Args:
            channels (Dict): A dictionary of channel names and their information.
            csv_tags (str): A comma-separated list of forbidden tags.

        Returns:
            Dict: A dictionary of channels excluding those with forbidden tags.
        """
        forbidden_tags = self._csv_to_set(csv_tags)
        return {k: v for k, v in channels.items() if not self._matches_any_tag(v, forbidden_tags)}

    def _keep_recently_active(self, channels: Dict, youtube_client, max_inactive_days: int) -> Dict:
        """
        Filters channels to keep only those recently active.

        Args:
            channels (Dict): A dictionary of channel names and their information.
            youtube_client: The YouTube API client used to fetch upload dates.
            max_inactive_days (int): The maximum number of days since the last upload to consider a channel active.

        Returns:
            Dict: A dictionary of channels that are recently active.
        """
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=max_inactive_days)
        still_active: Dict = {}
        for name, info in channels.items():
            last_dt = self.channel_manager.get_last_upload_date(youtube_client, info["channel_id"])
            if last_dt and last_dt >= cutoff:
                still_active[name] = info
            else:
                logger.info("Skipping %s (last upload %s)", name, last_dt.date() if last_dt else "never")
        return still_active