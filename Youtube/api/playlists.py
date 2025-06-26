# File: Youtube/api/playlists.py

import json
import logging
import os
import pathlib
from typing import List, Tuple, Optional

from api.metadata import MetadataManager

logger = logging.getLogger(__name__)


class PlaylistManager:
    """
    Manages playlists and videos using YouTube API interactions.

    This class provides methods to generate playlists, search for videos within
    a channel, and retrieve video IDs from playlists. It also supports caching
    playlist search results for efficiency.

    Attributes:
        metadata (Optional[MetadataManager]): An instance of MetadataManager to
            fetch video metadata.
    """

    def __init__(self, metadata_manager: Optional[MetadataManager] = None):
        """
        Initializes the PlaylistManager.

        Args:
            metadata_manager (Optional[MetadataManager]): An optional instance of
                MetadataManager for fetching video metadata.
        """
        self.metadata = metadata_manager

    def generate_videos_by_search(
            self,
            youtube_client,
            channel_id: str,
            keyword: str,
            max_results: int = 50,
    ):
        """
        Searches for videos in a channel based on a keyword.

        Args:
            youtube_client: The YouTube API client used to perform the search.
            channel_id (str): The ID of the channel to search within.
            keyword (str): The keyword to search for.
            max_results (int): The maximum number of results per page. Defaults to 50.

        Yields:
            Tuple: A tuple containing video ID, YouTube service, video title,
            channel name, and publish date if metadata is available. Otherwise,
            yields video ID and YouTube service.
        """
        page_token = None
        while True:
            def _req(svc):
                return svc.search().list(
                    part="id",
                    channelId=channel_id,
                    q=keyword,
                    type="video",
                    maxResults=max_results,
                    order="date",
                    pageToken=page_token,
                )

            resp, service = youtube_client.retry_request(_req)
            if not resp:
                break

            for item in resp.get("items", []):
                vid = item["id"]["videoId"]
                if self.metadata is not None:
                    v_title, ch_name, pub_dt, _ = self.metadata.fetch_video_metadata(
                        youtube_client, vid
                    )
                    if not all([v_title, ch_name, pub_dt]):
                        continue
                    yield vid, service, v_title, ch_name, pub_dt
                else:
                    yield vid, service

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

    @staticmethod
    def generate_playlists(youtube_client, channel_id: str, keywords: List[str], max_results: int = 10):
        """
        Generates playlists from a channel that match specific keywords.

        Args:
            youtube_client: The YouTube API client used to fetch playlists.
            channel_id (str): The ID of the channel to fetch playlists from.
            keywords (List[str]): A list of keywords to filter playlists.
            max_results (int): The maximum number of results per page. Defaults to 10.

        Yields:
            Tuple[str, str]: A tuple containing playlist ID and playlist title.
        """
        page_token = None
        while True:
            def _req(svc):
                return svc.playlists().list(
                    part="id,snippet",
                    channelId=channel_id,
                    maxResults=max_results,
                    pageToken=page_token,
                )

            resp, service = youtube_client.retry_request(_req)
            if not resp:
                break

            for item in resp.get("items", []):
                title = item["snippet"]["title"].lower()
                if any(k.lower() in title for k in keywords):
                    yield item["id"], item["snippet"]["title"]

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

    @staticmethod
    def generate_videos(youtube_client, playlist_id: str, max_results: int = 50):
        """
        Retrieves video IDs from a playlist.

        Args:
            youtube_client: The YouTube API client used to fetch playlist items.
            playlist_id (str): The ID of the playlist to fetch videos from.
            max_results (int): The maximum number of results per page. Defaults to 50.

        Yields:
            str: The video ID of each video in the playlist.
        """
        page_token = None
        while True:
            def _req(svc):
                return svc.playlistItems().list(
                    part="snippet,contentDetails",
                    playlistId=playlist_id,
                    maxResults=max_results,
                    pageToken=page_token,
                )

            resp, service = youtube_client.retry_request(_req)
            if not resp:
                break

            for item in resp.get("items", []):
                yield item["contentDetails"]["videoId"]

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

    def cached_search_playlists(self, youtube_client, channel_id: str, keywords: List[str]) -> List[Tuple[str, str]]:
        """
        Searches for playlists matching keywords and caches the results.

        Args:
            youtube_client: The YouTube API client used to perform the search.
            channel_id (str): The ID of the channel to search within.
            keywords (List[str]): A list of keywords to filter playlists.

        Returns:
            List[Tuple[str, str]]: A list of tuples containing playlist IDs and titles.
        """
        cache_file = f"yt_cache/pl_{channel_id}.json"
        if os.path.exists(cache_file):
            with open(cache_file, "r", encoding="utf-8") as fp:
                return json.load(fp)

        playlists = list(self.generate_playlists(youtube_client, channel_id, keywords))
        if not playlists:
            def _search_req(svc):
                return svc.search().list(
                    part="snippet",
                    channelId=channel_id,
                    q=" | ".join(keywords),
                    type="playlist",
                    maxResults=5,
                    fields="items(id/playlistId,snippet/title)",
                )

            resp, service = youtube_client.retry_request(_search_req)
            if resp:
                playlists = [
                    (it["id"]["playlistId"], it["snippet"]["title"])
                    for it in resp.get("items", [])
                ]

        pathlib.Path("yt_cache").mkdir(exist_ok=True)
        with open(cache_file, "w", encoding="utf-8") as fp:
            json.dump(playlists, fp)

        return playlists