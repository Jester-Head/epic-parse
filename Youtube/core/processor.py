# File: Youtube/core/processor.py

import logging
from typing import List, Dict

from config import CUTOFF_DATE, CHANNELS, KEYWORDS

logger = logging.getLogger(__name__)


class YouTubeProcessor:
    """
    Processes YouTube data including metadata, comments, channels, and playlists.

    This class provides methods to fetch and process comments from playlists and channels,
    as well as to handle batch processing of multiple channels.
    """

    def __init__(self, youtube_client, metadata_manager, comment_manager,
                 channel_manager, playlist_manager):
        """
        Initializes the YouTubeProcessor with required managers and a YouTube API client.

        Args:
            youtube_client: The YouTube API client instance.
            metadata_manager: An instance of MetadataManager for handling video metadata.
            comment_manager: An instance of CommentManager for managing comments.
            channel_manager: An instance of ChannelManager for managing channel-related operations.
            playlist_manager: An instance of PlaylistManager for handling playlists.
        """
        self.youtube_client = youtube_client
        self.metadata = metadata_manager
        self.comments = comment_manager
        self.channels = channel_manager
        self.playlists = playlist_manager

    def get_comments_by_playlist(self, channel_id: str, db, keywords: List[str]):
        """
        Fetches comments from videos in playlists matching the given keywords.

        Args:
            channel_id (str): The ID of the channel to fetch playlists from.
            db: The database instance for storing comments.
            keywords (List[str]): A list of keywords to search for matching playlists.

        Returns:
            None
        """
        processed = set()
        playlists = self.playlists.cached_search_playlists(self.youtube_client, channel_id, keywords)

        if not playlists:
            logger.info("No matching playlists for %s", channel_id)
            return

        for pl_id, pl_title in playlists:
            logger.info("Processing playlist %s – %s", pl_id, pl_title)
            try:
                for vid in self.playlists.generate_videos(self.youtube_client, pl_id, max_results=100):
                    if vid in processed:
                        continue
                    processed.add(vid)
                    res = self.comments.fetch_comments_with_resume(
                        self.youtube_client, vid, channel_id, db,
                        max_results=100, initial_fetch_date=CUTOFF_DATE
                    )
                    if res["comments"]:
                        db.insert_comments(res["comments"])
            except Exception as exc:
                logger.error("Error processing playlist %s: %s", pl_id, exc)

    def get_all_channel_comments(self, channel_id: str, db, max_results: int = 100, cutoff_date=CUTOFF_DATE):
        """
        Fetches all comments from a channel, starting from the cutoff date.

        Args:
            channel_id (str): The ID of the channel to fetch comments from.
            db: The database instance for storing comments and progress.
            max_results (int): The maximum number of comments to fetch per page. Defaults to 100.
            cutoff_date: The date to start fetching comments from. Defaults to CUTOFF_DATE.

        Returns:
            None
        """
        from datetime import datetime, timezone
        from dateutil.parser import parse

        cutoff_dt = cutoff_date if isinstance(cutoff_date, datetime) else parse(cutoff_date)
        if cutoff_dt.tzinfo is None:
            cutoff_dt = cutoff_dt.replace(tzinfo=timezone.utc)

        progress_key = f"chan::{channel_id}"
        page_token = db.get_progress(progress_key)
        if page_token is None and db.progress_exists(progress_key):
            logger.debug("Channel %s up-to-date; skipping", channel_id)
            return

        def _chan_req(svc):
            """
            Constructs the API request for fetching channel information.

            Args:
                svc: The YouTube API service instance.

            Returns:
                The API request object.
            """
            return svc.channels().list(part="snippet", id=channel_id, maxResults=1)

        c_resp, service = self.youtube_client.retry_request(_chan_req)
        if not c_resp or not c_resp.get("items"):
            logger.warning("Channel %s not found", channel_id)
            return

        chan_name = c_resp["items"][0]["snippet"]["title"]

        while True:
            def _page_req(svc):
                """
                Constructs the API request for fetching comments from a channel.

                Args:
                    svc: The YouTube API service instance.

                Returns:
                    The API request object.
                """
                return svc.commentThreads().list(
                    part="snippet",
                    allThreadsRelatedToChannelId=channel_id,
                    maxResults=max_results,
                    order="time",
                    pageToken=page_token,
                )

            resp, service = self.youtube_client.retry_request(_page_req)
            if not resp or not resp.get("items"):
                db.save_progress(progress_key, None)
                break

            vids = {
                itm["snippet"]["topLevelComment"]["snippet"].get("videoId")
                for itm in resp["items"]
            }
            vids.discard(None)
            meta = self.metadata.batch_fetch_video_metadata(self.youtube_client, list(vids))

            rows = []
            for itm in resp["items"]:
                snip = itm["snippet"]["topLevelComment"]["snippet"]
                comment_dt = parse(snip["updatedAt"]).astimezone(timezone.utc)
                if comment_dt < cutoff_dt:
                    db.save_progress(progress_key, None)
                    return

                vid = snip.get("videoId")
                if not vid or vid not in meta:
                    continue

                title, pub_dt, owner_id = meta[vid]
                if owner_id != channel_id:
                    continue

                rows.append({
                    "video_id": vid,
                    "video_title": title,
                    "channel_id": channel_id,
                    "channel_name": chan_name,
                    "video_publish_date": pub_dt,
                    "comment_id": itm["id"],
                    "author": snip.get("authorDisplayName"),
                    "author_channel_id": snip.get("authorChannelId"),
                    "text": snip.get("textOriginal"),
                    "like_count": snip.get("likeCount"),
                    "published_at": snip.get("publishedAt"),
                    "updated_at": snip["updatedAt"],
                })

            if rows:
                db.insert_comments(rows)

            page_token = resp.get("nextPageToken")
            db.save_progress(progress_key, page_token)
            if not page_token:
                break

    def process_channels(self, db, channels: Dict = None, keywords: List[str] = None):
        """
        Processes a list of channels by fetching comments from playlists or all channel videos.

        Args:
            db: The database instance for storing comments and progress.
            channels (Dict, optional): A dictionary of channel names and their information. Defaults to CHANNELS.
            keywords (List[str], optional): A list of keywords for playlist search. Defaults to KEYWORDS.

        Returns:
            None
        """
        channels = channels or CHANNELS
        keywords = keywords if isinstance(keywords, list) else list(keywords or KEYWORDS)

        for name, info in channels.items():
            cid = info["channel_id"]
            if info.get("only_wow"):
                self.get_all_channel_comments(cid, db, max_results=100)
            else:
                self.get_comments_by_playlist(cid, db, keywords=keywords)