# File: Youtube/api/comments.py

import logging
from datetime import datetime
from typing import Dict

from dateutil.parser import parse

from config import CUTOFF_DATE

logger = logging.getLogger(__name__)


class CommentManager:
    """
    Manages operations related to YouTube comments, including fetching, parsing,
    and resuming comment retrieval.

    Attributes:
        metadata (MetadataManager): Instance for managing video metadata.
    """

    def __init__(self, metadata_manager):
        """
        Initializes the CommentManager with a metadata manager.

        Args:
            metadata_manager: An instance of MetadataManager for video metadata operations.
        """
        self.metadata = metadata_manager

    @staticmethod
    def get_most_recent_comment_date(db, channel_id: str, video_id: str, fallback_date: str):
        """
        Retrieves the most recent comment date from the database or a fallback date.

        Args:
            db: The database instance to query for the most recent comment.
            channel_id (str): The ID of the channel associated with the video.
            video_id (str): The ID of the video to retrieve the comment date for.
            fallback_date (str): The fallback date to use if no comments are found.

        Returns:
            datetime: The most recent comment date.
        """
        row = db.get_most_recent_comment(channel_id, video_id)
        return parse(row["updated_at"]) if row else parse(fallback_date)

    @staticmethod
    def fetch_comments_page(youtube_client, video_id: str, page_token: str, max_results: int):
        """
        Fetches a single page of comments for a video.

        Args:
            youtube_client: The YouTube API client used to fetch comments.
            video_id (str): The ID of the video to fetch comments for.
            page_token (str): The token for the current page of comments.
            max_results (int): The maximum number of comments to fetch per page.

        Returns:
            Tuple[List[Dict], Optional[str], object]: A tuple containing the list of comments,
            the next page token, and the YouTube service instance.
        """

        def _req(svc):
            """
            Constructs the API request for fetching comments.

            Args:
                svc: The YouTube API service instance.

            Returns:
                The API request object.
            """
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
        Fetches comments for a video with resume capability, using fallback metadata if necessary.

        Args:
            youtube_client: The YouTube API client used to fetch comments.
            video_id (str): The ID of the video to fetch comments for.
            channel_id (str): The ID of the channel associated with the video.
            db: The database instance to store progress and retrieve metadata.
            max_results (int): The maximum number of comments to fetch per page. Defaults to 100.
            initial_fetch_date (str): The initial date to use for fetching comments. Defaults to CUTOFF_DATE.
            ignore_progress (bool): Whether to ignore saved progress and start from the beginning. Defaults to False.
            **metadata_kwargs: Additional metadata arguments for the video.

        Returns:
            Dict: A dictionary containing the fetched comments and the YouTube service instance.
        """
        # Fallback metadata values
        fallback_metadata = {
            "video_title": f"Unknown Title ({video_id})",
            "channel_name": f"Unknown Channel ({channel_id})",
            "video_publish_date": parse(initial_fetch_date),
        }

        # Attempt to fetch metadata
        video_title = metadata_kwargs.get('video_title')
        channel_name = metadata_kwargs.get('channel_name')
        video_publish_date = metadata_kwargs.get('video_publish_date')

        if not all([video_title, channel_name, video_publish_date]):
            video_title, channel_name, video_publish_date, _ = self.metadata.fetch_video_metadata(
                youtube_client, video_id
            )

        # Use fallback values if metadata is still incomplete
        video_title = video_title or fallback_metadata["video_title"]
        channel_name = channel_name or fallback_metadata["channel_name"]
        video_publish_date = video_publish_date or fallback_metadata["video_publish_date"]

        page_token = None if ignore_progress else db.get_progress(video_id)
        prev_token = "__first_pass"
        most_recent = self.get_most_recent_comment_date(db, channel_id, video_id, initial_fetch_date)

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
                c_date: datetime = parse(snip["updatedAt"])
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