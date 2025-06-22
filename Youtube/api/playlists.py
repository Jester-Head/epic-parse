import json
import logging
import os
import pathlib
from typing import List, Tuple, Optional

from Youtube.api.metadata import MetadataManager

logger = logging.getLogger(__name__)


class PlaylistManager:
    """
    Manages playlists and videos using YouTube API interactions.

    The PlaylistManager class provides utilities for interacting with YouTube APIs
    to search, fetch, and cache playlists and videos. It supports searching within
    channels, generating playlists based on keywords, retrieving videos from
    playlists, and caching search results for efficient reuse.

    Attributes:
        metadata (MetadataManager | None): Metadata manager used for fetching
            additional video information such as title, channel name, and publish
            date.
    """

    def __init__(self, metadata_manager: Optional["MetadataManager"] = None):
        """
        Parameters
        ----------
        metadata_manager : MetadataManager | None
            If provided, will be used to look up title, channel name and
            publish date for every video that is yielded.
        """
        self.metadata = metadata_manager

    def generate_videos_by_search(
            self,
            youtube_client,
            channel_id: str,
            keyword: str,
            max_results: int = 50,
    ):
        """Generate videos by searching within a channel."""
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
        """Generate playlists matching keywords."""
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
        """Generate video IDs from a playlist."""
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
        """Cache and retrieve playlists based on keywords."""
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
