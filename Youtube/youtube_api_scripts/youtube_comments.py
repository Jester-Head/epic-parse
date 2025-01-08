import atexit
import os
import time
import json
from dateutil.parser import parse
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import logging
import logging.config
from datetime import datetime

# Config variables and database connection
from config import API_KEY, CHANNELS, CUTOFF_DATE, KEYWORDS, LOG_CONFIG_PATH
from database_con import DatabaseConnection

# -----------------Logging Setup-----------------


def setup_logging(config_path=LOG_CONFIG_PATH):
    """
    Sets up logging configuration.
    """
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)

    with open(config_path, 'rt') as f:
        config = json.load(f)

    logging.config.dictConfig(config)


# Initialize logging before getting the logger
setup_logging()
logger = logging.getLogger(__name__)

# Load cache from file if exists
CACHE_FILE = 'video_metadata_cache.json'
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, 'r') as f:
        video_metadata_cache = json.load(f)
    logger.debug(f"Loaded video metadata cache from {CACHE_FILE}.")
else:
    video_metadata_cache = {}
    logger.debug("Initialized empty video metadata cache.")


def save_cache():
    with open(CACHE_FILE, 'w') as f:
        json.dump(video_metadata_cache, f)
    logger.debug(f"Saved video metadata cache to {CACHE_FILE}.")


# Ensure cache is saved on exit
atexit.register(save_cache)


def build_youtube_service(api_key=API_KEY):
    """
    Builds the YouTube API service client.

    Args:
        api_key (str): The API key to authenticate with the YouTube API.

    Returns:
        googleapiclient.discovery.Resource: The YouTube API client.
    """
    return build('youtube', 'v3', developerKey=api_key)

# -----------------Helpers-----------------


def retry_request(request, retries=5, backoff_factor=0.2):
    """
    Attempts to execute a given request with retries in case of server-side errors.

    Args:
        request: The request object that has an execute() method to be called.
        retries (int, optional): The number of times to retry the request. Defaults to 5.
        backoff_factor (float, optional): The factor by which the wait time increases after each retry. Defaults to 0.2.

    Returns:
        response: The successful response object if the request is executed successfully.
        None: If the request fails after all retries or encounters a non-retriable error.
    """
    for attempt in range(retries):
        try:
            response = request.execute()
            return response
        except HttpError as e:
            if e.resp.status in [500, 502, 503, 504]:
                sleep_time = backoff_factor * (2 ** attempt)
                logger.warning(
                    f"Request failed with status {e.resp.status}, retrying after {sleep_time:.2f} seconds..."
                )
                time.sleep(sleep_time)
            else:
                logger.error(
                    f"Request failed with status {e.resp.status} and error message: {e}"
                )
                return None
        except Exception as e:
            logger.error(f"Failed to execute request due to an error: {e}")
            return None

    logger.error(
        "All retries failed; the request could not be completed successfully."
    )
    return None


def fetch_video_metadata(youtube, video_id, channel_id):
    """
    Fetch video title and channel name for a given video and channel ID with caching.

    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        video_id (str): The ID of the YouTube video.
        channel_id (str): The ID of the YouTube channel.

    Returns:
        tuple: (video_title, channel_name)
    """
    global video_metadata_cache
    if video_id in video_metadata_cache:
        logger.debug(
            f"Video metadata for video ID {video_id} fetched from cache.")
        return video_metadata_cache[video_id]

    try:
        video_response = youtube.videos().list(part="snippet", id=video_id).execute()
        if not video_response["items"]:
            logger.warning(f"No video found with ID {video_id}.")
            return None, None
        video_title = video_response["items"][0]["snippet"]["title"]
        logger.info(f"Video Title: {video_title}")

        channel_response = youtube.channels().list(
            part="snippet", id=channel_id).execute()
        if not channel_response["items"]:
            logger.warning(f"No channel found with ID {channel_id}.")
            return None, None
        channel_name = channel_response["items"][0]["snippet"]["title"]
        logger.info(f"Channel Name: {channel_name}")

        # Cache the result
        video_metadata_cache[video_id] = (video_title, channel_name)

        return video_title, channel_name
    except Exception as e:
        logger.error(
            f"Failed to fetch video metadata for video ID {video_id} and channel ID {channel_id}: {e}"
        )
        return None, None


def get_most_recent_comment_date(db, channel_id, video_id, initial_fetch_date):
    """
    Get the most recent comment date for a video from the database.

    Args:
        db (DatabaseConnection): The database connection instance.
        channel_id (str): The ID of the YouTube channel.
        video_id (str): The ID of the YouTube video.
        initial_fetch_date (str): The initial date to start fetching comments from.

    Returns:
        datetime: The most recent comment date.
    """
    most_recent_comment = db.get_most_recent_comment(channel_id, video_id)
    return (
        parse(most_recent_comment["snippet"]
              ["topLevelComment"]["snippet"]["updatedAt"])
        if most_recent_comment
        else parse(initial_fetch_date)
    )


def fetch_comments_page(youtube, video_id, page_token, max_results):
    """
    Fetch a single page of comments for a video.

    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        video_id (str): The ID of the YouTube video.
        page_token (str): The page token for pagination.
        max_results (int): The maximum number of comments to fetch per API call.

    Returns:
        tuple: (fetched_comments, next_page_token)
    """
    try:
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            maxResults=max_results,
            order="time",
            pageToken=page_token,
        )
        response = retry_request(request)
        if response:
            fetched_comments = response.get("items", [])
            next_page_token = response.get("nextPageToken")
            return fetched_comments, next_page_token
        return [], None
    except Exception as e:
        logger.error(f"An error occurred while fetching comments page: {e}")
        return [], None


def get_progress(db, video_id):
    """
    Retrieve the last saved page token for a video from the database.

    Args:
        db (DatabaseConnection): The database connection instance.
        video_id (str): The ID of the YouTube video.

    Returns:
        str or None: The last page token or None if not found.
    """
    return db.get_progress(video_id)


def save_progress(db, video_id, page_token):
    """
    Save the current page token for a video to the database.

    Args:
        db (DatabaseConnection): The database connection instance.
        video_id (str): The ID of the YouTube video.
        page_token (str): The current page token.
    """
    db.save_progress(video_id, page_token)


def fetch_comments_with_resume(youtube, video_id, channel_id, db, max_results=100, initial_fetch_date=CUTOFF_DATE):
    """
    Fetches comments for a given YouTube video, including the video title and channel name, with resume capability.

    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        video_id (str): The ID of the YouTube video.
        channel_id (str): The ID of the YouTube channel.
        db (DatabaseConnection): The database connection instance.
        max_results (int, optional): The maximum number of comments to fetch per API call. Defaults to 100.
        initial_fetch_date (str, optional): The initial date to start fetching comments from. Defaults to CUTOFF_DATE.

    Returns:
        dict: A dictionary containing the video title, channel name, and a list of fetched comments.
    """
    logger.info(f"Fetching comments for video ID: {video_id}")
    all_comments = []

    # Fetch video metadata
    video_title, channel_name = fetch_video_metadata(
        youtube, video_id, channel_id)
    if not video_title or not channel_name:
        logger.warning(
            f"Skipping comment fetching for video ID: {video_id} due to missing metadata.")
        return {
            "video_title": video_title,
            "channel_name": channel_name,
            "comments": all_comments,
        }

    # Retrieve progress for resuming
    last_page_token = get_progress(db, video_id)
    page_token = last_page_token if last_page_token else None

    # Get the most recent comment date
    most_recent_comment_date = get_most_recent_comment_date(
        db, channel_id, video_id, initial_fetch_date)

    while True:
        try:
            # Fetch comment threads
            fetched_comments, page_token = fetch_comments_page(
                youtube, video_id, page_token, max_results
            )

            if not fetched_comments:
                break

            for comment in fetched_comments:
                comment_date_str = comment["snippet"]["topLevelComment"]["snippet"]["updatedAt"]
                comment_date = parse(comment_date_str)
                if comment_date <= most_recent_comment_date:
                    logger.info(
                        f"Reached already-fetched comments for video ID: {video_id}.")
                    if page_token:
                        # Save progress
                        save_progress(db, video_id, page_token)
                    return {
                        "video_title": video_title,
                        "channel_name": channel_name,
                        "comments": all_comments,
                    }

                # Enrich comment with video title and channel name
                comment["snippet"]["videoTitle"] = video_title
                comment["snippet"]["channelName"] = channel_name
                all_comments.append(comment)

            # Save progress after processing a page
            if page_token:
                save_progress(db, video_id, page_token)
            if not page_token:
                break

        except HttpError as e:
            if e.resp.status == 403 and "quotaExceeded" in str(e):
                logger.warning("Quota exceeded. Saving progress and stopping.")
                if page_token:
                    save_progress(db, video_id, page_token)  # Save progress
                return {
                    "video_title": video_title,
                    "channel_name": channel_name,
                    "comments": all_comments,
                }
            logger.error(
                f"An error occurred fetching comments for video {video_id}: {e}")
            break

    logger.info(
        f"Finished fetching comments for video ID: {video_id}. Total new comments: {len(all_comments)}")
    return {
        "video_title": video_title,
        "channel_name": channel_name,
        "comments": all_comments,
    }


def get_top_channels(youtube, channels=CHANNELS, n=3):
    """
    Retrieve the top `n` channels sorted by subscriber count.

    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        channels (dict): A dictionary of channel information where the key is the channel name and the value is a dictionary containing channel details, including the "handle".
        n (int, optional): The number of top channels to return. Defaults to 3.

    Returns:
        list: A list of tuples containing the top `n` channels sorted by subscriber count. Each tuple contains the channel name and its corresponding information dictionary.
    """
    for channel_name, channel_info in channels.items():
        try:
            request = youtube.channels().list(
                part="statistics",
                forHandle=channel_info["handle"]
            )
            response = retry_request(request)
            if response and "items" in response and response["items"]:
                subscriber_count = response["items"][0]["statistics"].get(
                    "subscriberCount", "0")
                channel_info["subscriber_count"] = int(subscriber_count)
                logger.info(
                    f"Channel: {channel_name}, Subscribers: {subscriber_count}")
            else:
                logger.warning(
                    f"Could not retrieve subscriber count for channel handle: {channel_info['handle']}")
                channel_info["subscriber_count"] = 0
        except Exception as e:
            logger.error(
                f"Error fetching subscriber count for channel handle {channel_info['handle']}: {e}")
            channel_info["subscriber_count"] = 0

    # Sort the channels by subscriber count
    sorted_channels = sorted(
        channels.items(),
        key=lambda x: x[1].get("subscriber_count", 0),
        reverse=True
    )
    return sorted_channels[:n]

# -----------------By Channel-----------------


def get_all_channel_comments(youtube, channel_id, db, max_results=100):
    """
    Fetches all unique comment threads for a given YouTube channel ID, includes video titles and channel name,
    and stores them in a database.
    """
    logger.info(f"Fetching all comment threads for channel ID: {channel_id}")

    # Fetch the channel name
    try:
        channel_response = youtube.channels().list(
            part="snippet",
            id=channel_id
        ).execute()
        if not channel_response["items"]:
            logger.warning(f"No channel found with ID {channel_id}.")
            return
        channel_name = channel_response["items"][0]["snippet"]["title"]
        logger.info(f"Channel Name: {channel_name}")
    except Exception as e:
        logger.error(
            f"Failed to fetch channel name for channel ID {channel_id}: {e}")
        channel_name = None

    page_token = None
    processed_videos = set()  # Track processed video IDs

    while True:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                allThreadsRelatedToChannelId=channel_id,
                maxResults=max_results,
                order="time",
                pageToken=page_token
            )
            response = retry_request(request)
            if not response:
                break

            comments = response.get("items", [])
            if comments:
                for comment in comments:
                    video_id = comment["snippet"]["videoId"]

                    if video_id in processed_videos:
                        logger.debug(
                            f"Already processed video ID: {video_id}. Skipping.")
                        continue  # Skip if already processed

                    processed_videos.add(video_id)  # Mark as processed

                    # Fetch comments with resume capability (includes video metadata)
                    result = fetch_comments_with_resume(
                        youtube,
                        video_id,
                        channel_id,
                        db,
                        max_results=max_results,
                        initial_fetch_date=CUTOFF_DATE
                    )
                    if result and result["comments"]:
                        db.insert_comments(result["comments"])

            page_token = response.get('nextPageToken')
            if not page_token:
                break

        except HttpError as e:
            logger.error(
                f"An error occurred processing channel {channel_id}: {e}"
            )
            break


# -----------------By Playlists-----------------


def generate_playlists(youtube, channel_id, keywords=KEYWORDS, max_results=10):
    """
    Generate playlists from a YouTube channel that match given keywords.

    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        channel_id (str): The ID of the YouTube channel.
        keywords (list or str, optional): A list of keywords or a single keyword to filter playlists. Defaults to KEYWORDS.
        max_results (int, optional): The maximum number of playlists to retrieve per API call. Defaults to 10.

    Yields:
        tuple: A tuple containing the playlist ID and playlist title for each matching playlist.
    """
    if not isinstance(keywords, list):
        keywords = list(keywords)

    page_token = None
    while True:
        try:
            request = youtube.playlists().list(
                part="id,snippet",
                channelId=channel_id,
                maxResults=max_results,
                pageToken=page_token
            )
            response = retry_request(request)
            if not response:
                break

            for item in response.get("items", []):
                playlist_title = item["snippet"]["title"].lower()
                # Check if any of the keywords appear in the playlist title
                if any(k.lower() in playlist_title for k in keywords):
                    yield item["id"], item["snippet"]["title"]

            page_token = response.get('nextPageToken')
            if not page_token:
                break

        except HttpError as e:
            logger.error(
                f"An error occurred while generating playlists for channel {channel_id}: {e}")
            break


def generate_videos_by_search(youtube, channel_id, search_keyword, max_results=50):
    """
    Generator to yield video IDs by searching with the given keyword within the channel.

    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        channel_id (str): The ID of the YouTube channel.
        search_keyword (str): The keyword to search in video titles.
        max_results (int, optional): The maximum number of results per API call. Defaults to 50.

    Yields:
        str: Video ID of each matching video.
    """
    page_token = None
    while True:
        try:
            request = youtube.search().list(
                part="id",
                channelId=channel_id,
                q=search_keyword,
                type="video",
                maxResults=max_results,
                order="date",
                pageToken=page_token
            )
            response = retry_request(request)
            if not response:
                break

            for item in response.get("items", []):
                video_id = item["id"]["videoId"]
                yield video_id

            page_token = response.get('nextPageToken')
            if not page_token:
                break

        except HttpError as e:
            logger.error(
                f"Error searching videos for keyword '{search_keyword}' in channel {channel_id}: {e}")
            break



def generate_videos(youtube, playlist_id, max_results=50):
    """
    Generator function to yield video IDs from a YouTube playlist.

    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        playlist_id (str): The ID of the YouTube playlist.
        max_results (int, optional): The maximum number of results to retrieve per API call. Defaults to 50.

    Yields:
        str: The video ID of each video in the playlist.

    Raises:
        Exception: If the request fails after several retries.
    """
    page_token = None
    while True:
        try:
            request = youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=playlist_id,
                maxResults=max_results,
                pageToken=page_token
            )
            response = retry_request(request)
            if not response:
                logger.error("Failed to fetch data after several retries.")
                break

            for item in response.get("items", []):
                yield item["contentDetails"]["videoId"]

            page_token = response.get('nextPageToken')
            if not page_token:
                break

        except HttpError as e:
            logger.error(
                f"An error occurred while generating videos for playlist {playlist_id}: {e}")
            break


def get_comments_by_playlist(youtube, channel_id, db, keywords=KEYWORDS, max_results=5):
    if not isinstance(keywords, list):
        keywords = list(keywords)

    processed_videos = set()  # Track processed video IDs

    try:
        playlists = list(generate_playlists(
            youtube, channel_id, keywords, max_results=max_results
        ))

        if not playlists:
            logger.info(
                f"No playlists found for channel ID {channel_id} with keywords {keywords}. Searching videos with the same keywords.")
            for keyword in keywords:
                videos = generate_videos_by_search(
                    youtube, channel_id, keyword, max_results=max_results)
                for video_id in videos:
                    if video_id in processed_videos:
                        logger.debug(
                            f"Already processed video ID: {video_id}. Skipping.")
                        continue
                    processed_videos.add(video_id)

                    logger.info(f"Fetching comments for video: {video_id}")
                    result = fetch_comments_with_resume(
                        youtube,
                        video_id,
                        channel_id,
                        db,
                        max_results=100,
                        initial_fetch_date=CUTOFF_DATE
                    )
                    if result and result["comments"]:
                        db.insert_comments(result["comments"])
            return

        for playlist_id, playlist_title in playlists:
            logger.info(
                f"Processing Playlist ID: {playlist_id}, Title: {playlist_title}")
            videos = generate_videos(youtube, playlist_id)
            for video_id in videos:
                if video_id in processed_videos:
                    logger.debug(
                        f"Already processed video ID: {video_id}. Skipping.")
                    continue
                processed_videos.add(video_id)

                logger.info(f"Fetching comments for video: {video_id}")
                result = fetch_comments_with_resume(
                    youtube,
                    video_id,
                    channel_id,
                    db,
                    max_results=100,
                    initial_fetch_date=CUTOFF_DATE
                )
                if result and result["comments"]:
                    db.insert_comments(result["comments"])
    except HttpError as e:
        logger.error(
            f"An error occurred processing channel {channel_id} with keywords '{keywords}': {e}")




# -----------------By Videos-----------------


def filter_videos(youtube, channel_id, db, keywords=KEYWORDS, max_results=100):
    """
    Filters videos from a YouTube channel based on specified keywords and fetches comments for each video.

    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        channel_id (str): The ID of the YouTube channel to filter videos from.
        db (DatabaseConnection): The database connection instance.
        keywords (list or str, optional): A list of keywords or a single keyword to filter videos by. Defaults to KEYWORDS.
        max_results (int, optional): The maximum number of results to return per page. Defaults to 20.

    Returns:
        None
    """
    if not isinstance(keywords, list):
        keywords = list(keywords)

    for keyword in keywords:
        logger.info(
            f"Filtering videos for channel {channel_id} by keyword '{keyword}'")
        page_token = None

        while True:
            try:
                request = youtube.search().list(
                    part="snippet",
                    channelId=channel_id,
                    q=keyword,
                    type="video",
                    maxResults=max_results,
                    order="date",
                    pageToken=page_token
                )
                response = retry_request(request)
                if not response:
                    break

                for item in response.get("items", []):
                    video_id = item["id"]["videoId"]
                    logger.info(f"Fetching comments for video: {video_id}")
                    result = fetch_comments_with_resume(
                        youtube,
                        video_id,
                        channel_id,
                        db,
                        max_results=100,
                        initial_fetch_date=CUTOFF_DATE
                    )
                    if result and result["comments"]:
                        db.insert_comments(result["comments"])

                page_token = response.get('nextPageToken')
                if not page_token:
                    break

            except HttpError as e:
                logger.error(
                    f"An error occurred processing channel {channel_id} with keyword '{keyword}': {e}"
                )
                break


def process_channels(youtube, db, channels=CHANNELS, keywords=KEYWORDS, limit_channels=None):
    """
    Processes YouTube channels to fetch comments based on specified keywords.

    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        db (DatabaseConnection): The database connection instance.
        channels (dict): A dictionary of channels to process. Defaults to CHANNELS.
        keywords (list or str): A list of keywords to filter comments. Defaults to KEYWORDS.
        limit_channels (int, optional): The number of top channels to process based on subscriber count. Defaults to None.

    Returns:
        None
    """
    if not isinstance(keywords, list):
        keywords = list(keywords)
    if limit_channels:
        # Limit the channels to process based on the top subscriber count
        top_channels = get_top_channels(youtube, channels, n=limit_channels)
        channels = dict(top_channels)
        logger.info(f"Fetching comments for channels: {list(channels.keys())}")

    for channel_name, channel_info in channels.items():
        channel_id = channel_info.get("channel_id")
        if not channel_id:
            logger.warning(
                f"No channel_id found for channel '{channel_name}'. Skipping.")
            continue

        if channel_info.get("only_wow", False):
            logger.info(
                f"Channel '{channel_name}' marked as 'only_wow'. Fetching all channel comments.")
            get_all_channel_comments(
                youtube, channel_id, db, max_results=100)
        else:
            logger.info(
                f"Channel '{channel_name}' not marked as 'only_wow'. Fetching comments by playlist.")
            # For "only_wow" = False, fetch by playlist using multiple keywords
            get_comments_by_playlist(
                youtube, channel_id, db, keywords, max_results=10)

# -----------------Main Execution-----------------



if __name__ == "__main__":
    # Example usage to fetch comments for a specific channel
    channel = CHANNELS['Asmongold']['channel_id']
    youtube_service = build_youtube_service()
    with DatabaseConnection() as db:
        get_comments_by_playlist(
            youtube_service,
            channel,
            db,
            keywords=KEYWORDS,
            max_results=10
        )
    
