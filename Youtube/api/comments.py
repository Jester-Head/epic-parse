# File: api/comments.py
import logging
from typing import Dict

from dateutil.parser import parse

from Youtube.config import CUTOFF_DATE

logger = logging.getLogger(__name__)


class CommentManager:
    """
    Manages operations related to YouTube comments, including fetching, parsing,
    and resuming comment retrieval.

    The class integrates with metadata management and provides utilities for handling
    YouTube's comment API, enabling robust and incremental data collection. Primarily
    handles video comments, ensuring metadata consistency and maintaining pagination state.

    Attributes:
        metadata: A metadata manager instance for retrieving or validating video/channel
            metadata.
    """

    def __init__(self, metadata_manager):
        self.metadata = metadata_manager

    @staticmethod
    def get_most_recent_comment_date(db, channel_id: str, video_id: str, fallback_date: str):
        """Get the most recent comment date from the database or fallback."""
        row = db.get_most_recent_comment(channel_id, video_id)
        return parse(row["updated_at"]) if row else parse(fallback_date)

    @staticmethod
    def fetch_comments_page(youtube_client, video_id: str, page_token: str, max_results: int):
        """Fetch a single page of comments."""

        def _req(svc):
            return svc.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=max_results,
                order="time",
                pageToken=page_token,
            )

        resp, service = youtube_client.retry_request(_req)
        if not resp:
            return [], None, service
        return resp.get("items", []), resp.get("nextPageToken"), service

    def fetch_comments_with_resume(
            self,
            youtube_client,
            video_id: str,
            channel_id: str,
            db,
            max_results: int = 100,
            initial_fetch_date: str = CUTOFF_DATE,
            ignore_progress: bool = False,
            **metadata_kwargs
    ) -> Dict:
        """
        Fetches comments with resume capability.

        Returns:
            Dict with keys 'comments' and 'youtube_service'
        """

        video_title = metadata_kwargs.get('video_title')
        channel_name = metadata_kwargs.get('channel_name')
        video_publish_date = metadata_kwargs.get('video_publish_date')

        if not all([video_title, channel_name, video_publish_date]):
            video_title, channel_name, video_publish_date, _ = self.metadata.fetch_video_metadata(
                youtube_client, video_id
            )

        if not all([video_title, channel_name, video_publish_date]):
            logger.warning("Skipping %s: incomplete metadata", video_id)
            return {"comments": [], "youtube_service": youtube_client.service}

        page_token = None if ignore_progress else db.get_progress(video_id)
        prev_token = "__first_pass"
        most_recent = self.get_most_recent_comment_date(
            db, channel_id, video_id, initial_fetch_date
        )

        results = []

        while True:
            if page_token is not None and page_token == prev_token:
                logger.warning('Page token "%s" repeated – aborting', page_token)
                break
            prev_token = page_token

            page, next_token, service = self.fetch_comments_page(
                youtube_client, video_id, page_token, max_results
            )
            if not page:
                db.save_progress(video_id, None)
                break

            new_rows = []
            for item in page:
                snip = item["snippet"]["topLevelComment"]["snippet"]
                c_date = parse(snip["updatedAt"])
                if c_date > most_recent and c_date >= video_publish_date:
                    new_rows.append({
                        "video_id": video_id,
                        "video_title": video_title,
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "video_publish_date": video_publish_date,
                        "comment_id": item["id"],
                        "author": snip.get("authorDisplayName"),
                        "author_channel_id": snip.get("authorChannelId", {}).get("value"),
                        "text": snip.get("textDisplay"),
                        "like_count": snip.get("likeCount"),
                        "published_at": snip.get("publishedAt"),
                        "updated_at": snip["updatedAt"],
                    })

            if new_rows:
                results.extend(new_rows)

            if next_token:
                db.save_progress(video_id, next_token)
                page_token = next_token
                continue
            else:
                db.save_progress(video_id, None)
                break

        return {"comments": results, "youtube_service": youtube_client.service}
