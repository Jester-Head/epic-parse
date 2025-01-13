from itertools import cycle
import atexit
import os
import time
import json
from dateutil.parser import parse
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import logging
import logging.config
import random
import threading
from functools import wraps

# Config variables and database connection
from config import CHANNELS, CUTOFF_DATE, KEYWORDS, LOG_CONFIG_PATH, API_KEYS
from database_con import DatabaseConnection

# -----------------Logging Setup-----------------


def setup_logging(config_path=LOG_CONFIG_PATH):
    """
    Sets up the logging configuration for the script.

    This function creates a 'logs' directory if it doesn't exist and configures the logging
    based on the JSON configuration file provided.

    Args:
        config_path (str, optional): Path to the JSON logging configuration file.
                                     Defaults to LOG_CONFIG_PATH from config.
    """
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)

    with open(config_path, 'rt') as f:
        config = json.load(f)

    logging.config.dictConfig(config)


# Initialize logging before getting the logger
setup_logging()
logger = logging.getLogger(__name__)

# -----------------Video Metadata Cache-----------------
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
    """
    Saves the current video metadata cache to a JSON file.

    This function serializes the `video_metadata_cache` dictionary and writes it to
    the `CACHE_FILE`. It is registered to run upon program exit to ensure that
    the cache is preserved.
    """
    with open(CACHE_FILE, 'w') as f:
        json.dump(video_metadata_cache, f)
    logger.debug(f"Saved video metadata cache to {CACHE_FILE}.")


# Ensure cache is saved on exit
atexit.register(save_cache)

# -----------------API Key Management-----------------
# Initialize a cycle iterator for API keys
api_key_cycle = cycle(API_KEYS)


def build_youtube_service():
    """
    Builds the YouTube API service client using the next available API key.

    This function cycles through the provided `API_KEYS` to distribute the quota
    usage across multiple keys. It also disables the discovery cache to prevent
    dependency on deprecated libraries.

    Returns:
        googleapiclient.discovery.Resource: An instance of the YouTube API service client.
    """
    api_key = next(api_key_cycle)
    logger.info(f"Using API Key: {api_key}")
    return build('youtube', 'v3', developerKey=api_key, cache_discovery=False)

# -----------------Helpers-----------------


# Define a semaphore to limit the number of concurrent requests
MAX_CONCURRENT_REQUESTS = 5
# Semaphore to limit the number of concurrent requests to avoid hitting API rate limits
semaphore = threading.Semaphore(MAX_CONCURRENT_REQUESTS)


def limit_concurrent_requests(semaphore):
    """
    Decorator to limit the number of concurrent executions of a function using a semaphore.

    This decorator ensures that no more than a specified number of threads can execute
    the decorated function simultaneously. It is useful for controlling the rate of API
    requests to prevent exceeding rate limits.

    Args:
        semaphore (threading.Semaphore): A semaphore object to control concurrency.

    Returns:
        function: The decorated function with concurrency control.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with semaphore:
                return func(*args, **kwargs)
        return wrapper
    return decorator


@limit_concurrent_requests(semaphore)
def retry_request(request_func, youtube_service, retries=5, backoff_factor=0.2):
    """
    Executes a YouTube API request with retry logic for handling transient errors.

    This function attempts to execute the provided `request_func` up to a specified
    number of retries in case of server-side errors or quota-related issues. It
    implements exponential backoff to mitigate the risk of overwhelming the API.

    Args:
        request_func (callable): A function that accepts `youtube_service` and returns a
                                 YouTube API request object.
        youtube_service (googleapiclient.discovery.Resource): The current YouTube API client.
        retries (int, optional): The maximum number of retry attempts. Defaults to 5.
        backoff_factor (float, optional): The base factor for calculating backoff delays.
                                          Defaults to 0.2.

    Returns:
        tuple:
            - response (dict or None): The API response if successful; otherwise, None.
            - youtube_service (googleapiclient.discovery.Resource): The (possibly updated)
              YouTube API client, especially after key rotations.
    """
    for attempt in range(retries):
        try:
            response = request_func(youtube_service).execute()
            return response, youtube_service
        except HttpError as e:
            error_reason = None
            try:
                error_content = e.content.decode('utf-8')
                error_json = json.loads(error_content)
                error_reason = error_json['error']['errors'][0]['reason']
                logger.error(f"Error Reason: {error_reason}")
                logger.error(f"Full Error Response: {error_json}")
            except (KeyError, json.JSONDecodeError, AttributeError) as parse_error:
                logger.error(f"Failed to parse error details: {parse_error}")

            if e.resp.status in [500, 502, 503, 504]:
                sleep_time = backoff_factor * (2 ** attempt)
                logger.warning(
                    f"Server error {e.resp.status}. Retrying after {sleep_time:.2f} seconds..."
                )
                time.sleep(sleep_time)
            elif e.resp.status == 403:
                if error_reason in ["quotaExceeded", "dailyLimitExceeded", "userRateLimitExceeded"]:
                    logger.warning(
                        "Quota-related error encountered. Switching to the next API key.")
                    try:
                        youtube_service = build_youtube_service()
                    except HttpError as e:
                        logger.error(f"Failed to build YouTube service: {e}")
                        return None, youtube_service
                    except Exception as e:
                        logger.error(f"An unexpected error occurred: {e}")
                        return None, youtube_service
                    # Reset the attempt counter after switching API keys
                    attempt = 0
                    # Random sleep to prevent synchronized retries
                    sleep_time = backoff_factor * \
                        (2 ** attempt) + random.uniform(0, 0.1)
                    time.sleep(sleep_time)
                else:
                    logger.error(
                        f"Access forbidden due to {error_reason}. Not rotating API keys.")
                    return None, youtube_service
            else:
                logger.error(
                    f"Request failed with status {e.resp.status} and reason {error_reason}: {e}"
                )
                return None, youtube_service
        except Exception as e:
            logger.error(
                f"Failed to execute request due to an unexpected error: {e}")
            return None, youtube_service

    logger.error(
        "All retries failed; the request could not be completed successfully."
    )
    return None, youtube_service


def fetch_video_metadata(youtube, video_id, channel_id):
    """
    Retrieves the title of a video and the name of its channel, utilizing caching to minimize API calls.

    This function first checks if the metadata for the given `video_id` is present in the
    `video_metadata_cache`. If not, it fetches the data from the YouTube Data API and
    updates the cache accordingly.

    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        video_id (str): The ID of the YouTube video.
        channel_id (str): The ID of the YouTube channel.

    Returns:
        tuple:
            - video_title (str or None): The title of the video if found; otherwise, None.
            - channel_name (str or None): The name of the channel if found; otherwise, None.
    """
    global video_metadata_cache
    if video_id in video_metadata_cache:
        logger.debug(
            f"Video metadata for video ID {video_id} fetched from cache."
        )
        return video_metadata_cache[video_id]

    try:
        def request_func(youtube_service):
            return youtube_service.videos().list(part="snippet", id=video_id)

        video_response, youtube_service = retry_request(request_func, youtube)
        if not video_response or not video_response.get("items"):
            logger.warning(f"No video found with ID {video_id}.")
            return None, None
        video_title = video_response["items"][0]["snippet"]["title"]
        logger.info(f"Video Title: {video_title}")

        def channel_request_func(youtube_service):
            return youtube_service.channels().list(part="snippet", id=channel_id)

        channel_response, youtube_service = retry_request(
            channel_request_func, youtube_service)
        if not channel_response or not channel_response.get("items"):
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
    Retrieves the most recent comment date for a specific video from the database.

    This function queries the database for the latest comment associated with the
    given `channel_id` and `video_id`. If no comments are found, it defaults to the
    provided `initial_fetch_date`.

    Args:
        db (DatabaseConnection): The database connection instance.
        channel_id (str): The ID of the YouTube channel.
        video_id (str): The ID of the YouTube video.
        initial_fetch_date (str): The initial date to start fetching comments from.

    Returns:
        datetime: The most recent comment date, parsed into a datetime object.
    """
    most_recent_comment = db.get_most_recent_comment(channel_id, video_id)
    return (
        parse(most_recent_comment["snippet"]
              ["topLevelComment"]["snippet"]["updatedAt"])
        if most_recent_comment
        else parse(initial_fetch_date)
    )


@limit_concurrent_requests(semaphore)
def fetch_comments_page(youtube, video_id, page_token, max_results):
    """
    Fetches a single page of comments for a specified YouTube video.

    This function retrieves a batch of comment threads associated with a video, ordered by time.
    It handles pagination through the `page_token` and limits the number of comments per request
    based on `max_results`.

    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        video_id (str): The ID of the YouTube video.
        page_token (str): The page token for pagination. Use `None` for the first page.
        max_results (int): The maximum number of comments to fetch per API call.

    Returns:
        tuple:
            - fetched_comments (list): A list of comment thread items retrieved.
            - next_page_token (str or None): The token for the next page of results, if any.
            - youtube_service (googleapiclient.discovery.Resource): The (possibly updated) YouTube API client.
    """
    try:
        def request_func(youtube_service):
            return youtube_service.commentThreads().list(
                part="snippet,replies",
                videoId=video_id,
                maxResults=max_results,
                order="time",
                pageToken=page_token,
            )

        response, youtube_service = retry_request(request_func, youtube)
        if response:
            fetched_comments = response.get("items", [])
            next_page_token = response.get("nextPageToken")
            return fetched_comments, next_page_token, youtube_service
        return [], None, youtube_service
    except Exception as e:
        logger.error(f"An error occurred while fetching comments page: {e}")
        return [], None, youtube_service


def get_progress(db, video_id):
    """
    Retrieves the last saved page token for a specific video from the database.

    This function is used to determine where to resume fetching comments in case the script
    was previously interrupted or paused.

    Args:
        db (DatabaseConnection): The database connection instance.
        video_id (str): The ID of the YouTube video.

    Returns:
        str or None: The last page token if found; otherwise, `None`.
    """
    return db.get_progress(video_id)


def save_progress(db, video_id, page_token):
    """
    Saves the current page token for a specific video to the database.

    This function updates the progress tracking in the database, allowing the script
    to resume fetching comments from the last processed page in future runs.

    Args:
        db (DatabaseConnection): The database connection instance.
        video_id (str): The ID of the YouTube video.
        page_token (str): The current page token to save.
    """
    db.save_progress(video_id, page_token)


def fetch_comments_with_resume(youtube, video_id, channel_id, db, max_results=100, initial_fetch_date=CUTOFF_DATE):
    """
    Fetches comments for a specific YouTube video with resume capability.

    This function retrieves comments from a video, enriching each comment with the video
    title and channel name. It respects previously fetched comments by comparing comment
    dates and resumes fetching from the last saved page token to avoid duplicate data.

    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        video_id (str): The ID of the YouTube video.
        channel_id (str): The ID of the YouTube channel.
        db (DatabaseConnection): The database connection instance.
        max_results (int, optional): The maximum number of comments to fetch per API call.
                                     Defaults to 100.
        initial_fetch_date (str, optional): The initial date to start fetching comments from.
                                            Defaults to `CUTOFF_DATE` from config.

    Returns:
        dict: A dictionary containing:
              - "video_title" (str or None): The title of the video.
              - "channel_name" (str or None): The name of the channel.
              - "comments" (list): A list of fetched comment threads.
              - "youtube_service" (googleapiclient.discovery.Resource): The (possibly updated) YouTube API client.
    """
    logger.info(f"Fetching comments for video ID: {video_id}")
    all_comments = []

    # Fetch video metadata
    video_title, channel_name = fetch_video_metadata(
        youtube, video_id, channel_id)
    if not video_title or not channel_name:
        logger.warning(
            f"Skipping comment fetching for video ID: {video_id} due to missing metadata."
        )
        return {
            "video_title": video_title,
            "channel_name": channel_name,
            "comments": all_comments,
            "youtube_service": youtube  # Include updated service
        }

    # Retrieve progress for resuming
    last_page_token = get_progress(db, video_id)
    page_token = last_page_token if last_page_token else None

    # Get the most recent comment date
    most_recent_comment_date = get_most_recent_comment_date(
        db, channel_id, video_id, initial_fetch_date
    )

    while True:
        try:
            # Fetch comment threads
            fetched_comments, next_page_token, youtube = fetch_comments_page(
                youtube, video_id, page_token, max_results
            )

            if not fetched_comments:
                break

            for comment in fetched_comments:
                comment_date_str = comment["snippet"]["topLevelComment"]["snippet"]["updatedAt"]
                comment_date = parse(comment_date_str)
                if comment_date <= most_recent_comment_date:
                    logger.info(
                        f"Reached already-fetched comments for video ID: {video_id}."
                    )
                    if page_token:
                        # Save progress
                        save_progress(db, video_id, page_token)
                    return {
                        "video_title": video_title,
                        "channel_name": channel_name,
                        "comments": all_comments,
                        "youtube_service": youtube  # Include updated service
                    }

                # Enrich comment with video title and channel name
                comment["snippet"]["videoTitle"] = video_title
                comment["snippet"]["channelName"] = channel_name
                all_comments.append(comment)

            # Save progress after processing a page
            if next_page_token:
                save_progress(db, video_id, next_page_token)
                page_token = next_page_token
            else:
                break

        except HttpError as e:
            # The retry_request function already handles quotaExceeded and other errors
            logger.error(
                f"An error occurred fetching comments for video {video_id}: {e}"
            )
            break

    logger.info(
        f"Finished fetching comments for video ID: {video_id}. Total new comments: {len(all_comments)}"
    )
    return {
        "video_title": video_title,
        "channel_name": channel_name,
        "comments": all_comments,
        "youtube_service": youtube  # Include updated service
    }


def get_top_channels(youtube, channels=CHANNELS, n=3):
    """
    Retrieves the top `n` YouTube channels sorted by subscriber count.

    This function iterates through the provided `channels` dictionary, fetches the
    subscriber count for each channel using its handle, and returns the top `n`
    channels based on the highest subscriber counts.

    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        channels (dict, optional): A dictionary of channel information where the key is
                                   the channel name and the value is a dictionary containing
                                   channel details, including the "handle". Defaults to CHANNELS.
        n (int, optional): The number of top channels to return based on subscriber count.
                           Defaults to 3.

    Returns:
        list: A list of tuples, each containing the channel name and its corresponding information
              dictionary, sorted in descending order of subscriber count. The list contains up to
              `n` channels.
    """
    for channel_name, channel_info in channels.items():
        try:
            def request_func(youtube_service):
                return youtube_service.channels().list(
                    part="statistics",
                    # Valid as per documentation
                    forHandle=channel_info["handle"]
                )

            response, youtube = retry_request(request_func, youtube)
            if response and "items" in response and response["items"]:
                subscriber_count = response["items"][0]["statistics"].get(
                    "subscriberCount", "0"
                )
                channel_info["subscriber_count"] = int(subscriber_count)
                logger.info(
                    f"Channel: {channel_name}, Subscribers: {subscriber_count}"
                )
            else:
                logger.warning(
                    f"Could not retrieve subscriber count for channel handle: {channel_info['handle']}"
                )
                channel_info["subscriber_count"] = 0
        except Exception as e:
            logger.error(
                f"Error fetching subscriber count for channel handle {channel_info['handle']}: {e}"
            )
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
    Fetches all unique comment threads for a specific YouTube channel.

    This function retrieves all comment threads related to a given channel by fetching
    comments from all videos associated with the channel. It enriches each comment
    with the video title and channel name before storing them in the database.

    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        channel_id (str): The ID of the YouTube channel.
        db (DatabaseConnection): The database connection instance.
        max_results (int, optional): The maximum number of comment threads to fetch per API call.
                                     Defaults to 100.

    Returns:
        None
    """
    logger.info(f"Fetching all comment threads for channel ID: {channel_id}")

    # Fetch the channel name
    try:
        def channel_request_func(youtube_service):
            return youtube_service.channels().list(
                part="snippet",
                id=channel_id
            )

        channel_response, youtube_service = retry_request(
            channel_request_func, youtube)
        if not channel_response["items"]:
            logger.warning(f"No channel found with ID {channel_id}.")
            return
        channel_name = channel_response["items"][0]["snippet"]["title"]
        logger.info(f"Channel Name: {channel_name}")
    except Exception as e:
        logger.error(
            f"Failed to fetch channel name for channel ID {channel_id}: {e}"
        )
        channel_name = None

    page_token = None
    processed_videos = set()  # Track processed video IDs

    while True:
        try:
            def request_func(youtube_service):
                return youtube_service.commentThreads().list(
                    part="snippet",
                    allThreadsRelatedToChannelId=channel_id,
                    maxResults=max_results,
                    order="time",
                    pageToken=page_token
                )

            response, youtube_service = retry_request(
                request_func, youtube_service)
            if not response:
                break

            comments = response.get("items", [])
            if comments:
                for comment in comments:
                    video_id = comment["snippet"]["videoId"]

                    if video_id in processed_videos:
                        logger.debug(
                            f"Already processed video ID: {video_id}. Skipping."
                        )
                        continue  # Skip if already processed

                    processed_videos.add(video_id)  # Mark as processed

                    # Fetch comments with resume capability (includes video metadata)
                    result = fetch_comments_with_resume(
                        youtube_service,
                        video_id,
                        channel_id,
                        db,
                        max_results=max_results,
                        initial_fetch_date=CUTOFF_DATE
                    )
                    # Update youtube_service if rotated
                    youtube_service = result.get(
                        "youtube_service", youtube_service)
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

# -----------------By Videos-----------------


@limit_concurrent_requests(semaphore)
def generate_videos_by_search(youtube, channel_id, search_keyword, max_results=50):
    """
    Generates video IDs by searching within a specific YouTube channel using a keyword.

    This generator function performs a search query within the specified channel for videos
    that match the provided `search_keyword`. It yields video IDs one by one.

    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        channel_id (str): The ID of the YouTube channel to search within.
        search_keyword (str): The keyword to search for in video titles.
        max_results (int, optional): The maximum number of search results to fetch per API call.
                                     Defaults to 50.

    Yields:
        str: The ID of each matching YouTube video.
    """
    page_token = None
    while True:
        try:
            def request_func(youtube_service):
                return youtube_service.search().list(
                    part="id",
                    channelId=channel_id,
                    q=search_keyword,
                    type="video",
                    maxResults=max_results,
                    order="date",
                    pageToken=page_token
                )

            response, youtube_service = retry_request(request_func, youtube)
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
                f"Error searching videos for keyword '{search_keyword}' in channel {channel_id}: {e}"
            )
            break

# -----------------By Playlists-----------------


@limit_concurrent_requests(semaphore)
def generate_playlists(youtube, channel_id, keywords=KEYWORDS, max_results=10):
    """
    Generates playlist IDs and titles that match specified keywords within a YouTube channel.

    This generator function retrieves playlists from a given channel and yields those
    whose titles contain any of the specified `keywords`.

    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        channel_id (str): The ID of the YouTube channel.
        keywords (list or str, optional): A list of keywords to filter playlists by title.
                                          Defaults to `KEYWORDS` from config.
        max_results (int, optional): The maximum number of playlists to retrieve per API call.
                                     Defaults to 10.

    Yields:
        tuple: A tuple containing:
               - playlist_id (str): The ID of the matching playlist.
               - playlist_title (str): The title of the matching playlist.
    """
    if not isinstance(keywords, list):
        keywords = list(keywords)

    page_token = None
    while True:
        try:
            def request_func(youtube_service):
                return youtube_service.playlists().list(
                    part="id,snippet",
                    channelId=channel_id,
                    maxResults=max_results,
                    pageToken=page_token
                )

            response, youtube_service = retry_request(request_func, youtube)
            if not response:
                break

            for item in response.get("items", []):
                playlist_title = item["snippet"]["title"].lower()
                if any(k.lower() in playlist_title for k in keywords):
                    yield item["id"], item["snippet"]["title"]

            page_token = response.get('nextPageToken')
            if not page_token:
                break

        except HttpError as e:
            logger.error(
                f"An error occurred while generating playlists for channel {channel_id}: {e}"
            )
            break


@limit_concurrent_requests(semaphore)
def generate_videos(youtube, playlist_id, max_results=50):
    """
    Generates video IDs from a specified YouTube playlist.

    This generator function retrieves all videos within a given playlist and yields their IDs.

    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        playlist_id (str): The ID of the YouTube playlist.
        max_results (int, optional): The maximum number of playlist items to retrieve per API call.
                                     Defaults to 50.

    Yields:
        str: The ID of each video in the playlist.
    """
    page_token = None
    while True:
        try:
            def request_func(youtube_service):
                return youtube_service.playlistItems().list(
                    part="snippet,contentDetails",
                    playlistId=playlist_id,
                    maxResults=max_results,
                    pageToken=page_token
                )

            response, youtube_service = retry_request(request_func, youtube)
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
                f"An error occurred while generating videos for playlist {playlist_id}: {e}"
            )
            break


def get_comments_by_playlist(youtube, channel_id, db, keywords=KEYWORDS, max_results=5):
    """
    Fetches comments from YouTube videos within playlists that match specified keywords.

    This function first attempts to retrieve playlists matching the provided `keywords`. If no
    such playlists are found, it falls back to searching for videos using the same keywords and
    fetching their comments. Comments are enriched with video titles and channel names before
    being stored in the database.

    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        channel_id (str): The ID of the YouTube channel to search within.
        db (DatabaseConnection): The database connection instance.
        keywords (list or str, optional): A list of keywords to filter playlists and videos by.
                                          Defaults to `KEYWORDS` from config.
        max_results (int, optional): The maximum number of playlists or videos to retrieve per API call.
                                     Defaults to 5.

    Returns:
        None
    """
    if not isinstance(keywords, list):
        keywords = list(keywords)

        processed_videos = set()  # Track processed video IDs

        try:
            playlists = list(generate_playlists(
                youtube, channel_id, keywords, max_results=max_results
            ))

            if not playlists:
                logger.info(
                    f"No playlists found for channel ID {channel_id} with keywords {keywords}. Searching videos with the same keywords."
                )
                for keyword in keywords:
                    videos = generate_videos_by_search(
                        youtube, channel_id, keyword, max_results=max_results
                    )
                    for video_id in videos:
                        if video_id in processed_videos:
                            logger.debug(
                                f"Already processed video ID: {video_id}. Skipping."
                            )
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
                        # Update youtube_service if rotated
                        youtube = result.get("youtube_service", youtube)
                        if result and result["comments"]:
                            db.insert_comments(result["comments"])
                return

            for playlist_id, playlist_title in playlists:
                logger.info(
                    f"Processing Playlist ID: {playlist_id}, Title: {playlist_title}"
                )
                videos = generate_videos(youtube, playlist_id)
                for video_id in videos:
                    if video_id in processed_videos:
                        logger.debug(
                            f"Already processed video ID: {video_id}. Skipping."
                        )
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
                    # Update youtube_service if rotated
                    youtube = result.get("youtube_service", youtube)
                    if result and result["comments"]:
                        db.insert_comments(result["comments"])
        except HttpError as e:
            logger.error(
                f"An error occurred processing channel {channel_id} with keywords '{keywords}': {e}"
            )

# -----------------Main Execution-----------------


def process_channels(youtube, db, channels=CHANNELS, keywords=KEYWORDS, limit_channels=None):
    """
    Processes multiple YouTube channels to fetch comments based on specified keywords.

    This function iterates through a dictionary of channels, optionally limiting the number
    of channels based on subscriber count. For each channel, it determines whether to
    fetch all comments or filter them based on playlists and keywords before storing them
    in the database.

    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        db (DatabaseConnection): The database connection instance.
        channels (dict, optional): A dictionary of channels to process, where the key is the
                                   channel name and the value is a dictionary containing channel
                                   details. Defaults to `CHANNELS` from config.
        keywords (list or str, optional): A list of keywords to filter comments by. Defaults to `KEYWORDS` from config.
        limit_channels (int, optional): The number of top channels to process based on subscriber count.
                                        If `None`, all channels are processed. Defaults to None.

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
                f"No channel_id found for channel '{channel_name}'. Skipping."
            )
            continue

        if channel_info.get("only_wow", False):
            logger.info(
                f"Channel '{channel_name}' marked as 'only_wow'. Fetching all channel comments."
            )
            get_all_channel_comments(
                youtube, channel_id, db, max_results=100
            )
        else:
            logger.info(
                f"Channel '{channel_name}' not marked as 'only_wow'. Fetching comments by playlist."
            )
            # For "only_wow" = False, fetch by playlist using multiple keywords
            get_comments_by_playlist(
                youtube, channel_id, db, keywords, max_results=10
            )


def main():
    """
    The main entry point for the YouTube comments fetching script.

    This function initializes the YouTube API client, establishes a connection to the
    database, and initiates the process of fetching comments for the specified channels
    based on configured keywords and limits.

    Returns:
        None
    """
    youtube_service = build_youtube_service()
    with DatabaseConnection() as db:
        process_channels(youtube_service, db, limit_channels=len(CHANNELS))


if __name__ == "__main__":
    main()
