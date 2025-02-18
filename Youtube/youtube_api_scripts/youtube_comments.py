import atexit
import json
import logging
import logging.config
import os
import random
import time
from collections import OrderedDict
from datetime import datetime
from itertools import cycle

from dateutil.parser import parse
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Config variables and database connection
from config import CHANNELS, CUTOFF_DATE, KEYWORDS, LOG_CONFIG_PATH, API_KEYS
from database_con import DatabaseConnection


# -----------------Logging Setup-----------------
def setup_logging(config_path=LOG_CONFIG_PATH):
    """
    Sets up the logging configuration from a configuration file.

    This function reads a logging configuration in JSON format from the specified
    path, creates the necessary logging directories if they do not exist, and applies
    the configuration using Python's `logging.config.dictConfig`.

    Args:
        config_path (str): Path to the logging configuration file in JSON format.
    """
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)

    with open(config_path, 'rt') as f:
        config = json.load(f)

    logging.config.dictConfig(config)


# Initialize logging before getting the logger
setup_logging()
logger = logging.getLogger(__name__)


# -----------------Video and Channel Metadata Cache-----------------
class LRUCache(OrderedDict):
    """
    Implements a Least Recently Used (LRU) Cache using an ordered dictionary.

    This class extends `OrderedDict` to provide an LRU caching mechanism. It
    automatically removes the least recently used item when the cache reaches
    its specified maximum size. Items are stored in the dictionary while
    retaining their insertion order.

    Attributes:
        max_size (int): The maximum number of items the cache can hold. When this
            number is exceeded, the least recently used item is automatically
            evicted.
    """
    def __init__(self, max_size):
        self.max_size = max_size
        super().__init__()

    def __setitem__(self, key, value):
        if len(self) >= self.max_size:
            self.popitem(last=False)
        super().__setitem__(key, value)


# Initialize caches with a maximum size
MAX_CACHE_SIZE = 1000

# --- Use os.path.join for correct path construction ---
VIDEO_CACHE_FILE = os.path.join('Youtube','yt_cache', 'video_metadata_cache.json')
CHANNEL_CACHE_FILE = os.path.join('Youtube','yt_cache', 'channel_metadata_cache.json')
# --- End of path changes ---

if os.path.exists(VIDEO_CACHE_FILE):
    with open(VIDEO_CACHE_FILE, 'r') as f:
        video_metadata_cache = LRUCache(MAX_CACHE_SIZE)
        video_metadata_cache.update(json.load(f))
    logger.debug(f"Loaded video metadata cache from {VIDEO_CACHE_FILE}.")
else:
    video_metadata_cache = LRUCache(MAX_CACHE_SIZE)
    logger.debug("Initialized empty video metadata cache.")

if os.path.exists(CHANNEL_CACHE_FILE):
    with open(CHANNEL_CACHE_FILE, 'r') as f:
        channel_metadata_cache = LRUCache(MAX_CACHE_SIZE)
        channel_metadata_cache.update(json.load(f))
    logger.debug(f"Loaded channel metadata cache from {CHANNEL_CACHE_FILE}.")
else:
    channel_metadata_cache = LRUCache(MAX_CACHE_SIZE)
    logger.debug("Initialized empty channel metadata cache.")


def save_caches():
    """
    Saves the video and channel metadata caches to their respective files.

    This function ensures that the directories for the cache files exist,
    and then saves the video and channel metadata caches using the custom
    serialization logic. The metadata is written in JSON format.

    Raises:
        TypeError: If an object being serialized is not of a supported type.
    """

    def custom_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    os.makedirs(os.path.dirname(VIDEO_CACHE_FILE), exist_ok=True)
    with open(VIDEO_CACHE_FILE, 'w') as f:
        json.dump(dict(video_metadata_cache), f, default=custom_serializer)
    logger.debug(f"Saved video metadata cache to {VIDEO_CACHE_FILE}.")

    # Ensure that the directory for the channel cache file exists
    os.makedirs(os.path.dirname(CHANNEL_CACHE_FILE), exist_ok=True)
    with open(CHANNEL_CACHE_FILE, 'w') as f:
        json.dump(dict(channel_metadata_cache), f, default=custom_serializer)
    logger.debug(f"Saved channel metadata cache to {CHANNEL_CACHE_FILE}.")


# Ensure caches are saved on exit
atexit.register(save_caches)

# -----------------API Key Management-----------------
# Initialize a cycle iterator for API keys
api_key_cycle = cycle(API_KEYS)
last_global_exhaustion_time = 0
GLOBAL_BACKOFF_TIME = 60 * 5  # 5 minutes


def build_youtube_service():
    """
    Builds and initializes a YouTube service client using the YouTube Data API v3.

    The function retrieves the next API key from a defined cycle of API keys
    and sets up the YouTube service client for making API requests. Additionally,
    it logs the API key being used for traceability.

    Returns:
        googleapiclient.discovery.Resource: A resource object with methods to interact
        with the YouTube Data API v3.

    Raises:
        Some specific exceptions may be raised implicitly, such as those related
        to invalid API key or client setup issues.
    """
    api_key = next(api_key_cycle)
    logger.info(f"Using API Key: {api_key}")
    return build('youtube', 'v3', developerKey=api_key, cache_discovery=False)


# -----------------Helpers-----------------
# Custom exception to signal that all keys are exhausted.
class QuotaExhaustedError(Exception):
    """
    Represents an error indicating that a quota has been exhausted.

    QuotaExhaustedError is a custom exception used to signal that a resource or
    usage quota has reached its maximum limit. This exception can be employed in
    contexts where quota limitations are enforced to restrict operations or resource
    usage in systems, APIs, or applications.
    """
    pass


def retry_request(request_func, youtube_service, retries=5, backoff_factor=0.2):
    """
    Retries a given YouTube API request with exponential backoff and API key rotation
    to handle errors such as quota issues or server-related problems.

    This function attempts to make a request to the YouTube API using the provided request
    function and service object. If errors occur due to server issues or quota limits, it
    will retry the request with exponential backoff and rotate through available API keys
    to minimize downtime. If all retries and key rotations are exhausted, the function
    returns a failure response.

    Args:
        request_func: Callable
            A function that takes a YouTube service object and returns a built request
            object using the YouTube API client library.
        youtube_service: build
            An instance of the YouTube service object created using the Google API
            client library to execute API requests.
        retries: int, optional
            The number of retry attempts allowed for the request. Defaults to 5.
        backoff_factor: float, optional
            A factor to calculate exponential backoff timing between retries. Defaults
            to 0.2.

    Returns:
        tuple:
            A tuple containing the API response and the YouTube service object. If the
            request fails after all retry attempts, the response part of the tuple will
            be `None`.

    Raises:
        QuotaExhaustedError:
            Raised when all available API keys are exhausted due to repeated quota
            errors, including "quotaExceeded", "dailyLimitExceeded", or
            "userRateLimitExceeded".
    """
    global last_global_exhaustion_time
    key_rotations = 0
    total_keys = len(API_KEYS)

    for attempt in range(retries):
        # If all keys have been rotated and we are still within the global backoff period,
        # wait until the backoff period has passed.
        if key_rotations >= total_keys and (time.time() - last_global_exhaustion_time) < GLOBAL_BACKOFF_TIME:
            remaining_wait = GLOBAL_BACKOFF_TIME - (time.time() - last_global_exhaustion_time)
            logger.warning(f"All API keys exhausted. Waiting for {remaining_wait:.2f} seconds before retrying.")
            time.sleep(remaining_wait)
            key_rotations = 0  # reset rotations after waiting

        try:
            response = request_func(youtube_service).execute()
            return response, youtube_service

        except HttpError as e:
            error_reason = None
            try:
                error_json = json.loads(e.content.decode('utf-8'))
                error_reason = error_json['error']['errors'][0]['reason']
                logger.error(f"Error Reason: {error_reason}")
                logger.error(f"Full Error Response: {error_json}")
            except (KeyError, json.JSONDecodeError, AttributeError) as parse_error:
                logger.error(f"Failed to parse error details: {parse_error}")

            # Handle server errors (500-series) with exponential backoff.
            if e.resp.status in [500, 502, 503, 504]:
                sleep_time = backoff_factor * (2 ** attempt)
                logger.warning(f"Server error {e.resp.status}. Retrying after {sleep_time:.2f} seconds.")
                time.sleep(sleep_time)

            # Handle quota errors (403 with quota-related reasons)
            elif e.resp.status == 403 and error_reason in ["quotaExceeded", "dailyLimitExceeded",
                                                           "userRateLimitExceeded"]:
                logger.warning("Quota error encountered. Rotating API key.")
                key_rotations += 1

                if key_rotations >= total_keys:
                    last_global_exhaustion_time = time.time()
                    logger.error("All API keys exhausted.")
                    raise QuotaExhaustedError("All API keys exhausted due to quota errors.")

                # Rotate to a new API key.
                youtube_service = build_youtube_service()
                sleep_delay = backoff_factor * (2 ** attempt) + random.uniform(0, 0.1)
                logger.info(f"Sleeping for {sleep_delay:.2f} seconds before retrying with a new key.")
                time.sleep(sleep_delay)

            else:
                logger.error(f"Access forbidden or other error: {error_reason}. Not retrying.")
                return None, youtube_service

        except Exception as ex:
            logger.error(f"Exception during request execution: {ex}")
            return None, youtube_service

    logger.error("All retries failed.")
    return None, youtube_service


def fetch_video_metadata(youtube, video_id, channel_id):
    """
    Fetches metadata for a YouTube video, including its title and the channel name.

    This function retrieves video metadata such as the video title and channel name
    from the YouTube Data API. It utilizes caching mechanisms to reduce redundant
    API requests. If metadata for a given video ID is already cached, it is retrieved
    from the local cache instead of sending a new API request. Similarly, channel
    names are also cached to avoid repetitive API calls. If metadata cannot be
    retrieved for any reason (e.g., if the video ID or channel ID is invalid), it
    returns None for both video and channel metadata.

    Args:
        youtube (object): The authenticated YouTube API client.
        video_id (str): The unique identifier for the YouTube video.
        channel_id (str): The unique identifier for the YouTube channel.

    Returns:
        tuple: A tuple containing:
            - video_title (str or None): The title of the video, or None if it could
              not be retrieved.
            - channel_name (str or None): The name of the channel, or None if it
              could not be retrieved.
    """
    global video_metadata_cache, channel_metadata_cache

    # Check if video metadata is already cached
    if video_id in video_metadata_cache:
        logger.debug(
            f"Video metadata for video ID {video_id} fetched from cache.")
        return video_metadata_cache[video_id]

    try:
        # Fetch video details
        def video_request_func(youtube_service):
            return youtube_service.videos().list(part="snippet", id=video_id)

        video_response, youtube_service = retry_request(
            video_request_func, youtube)
        if not video_response or not video_response.get("items"):
            logger.warning(f"No video found with ID {video_id}.")
            return None, None

        video_title = video_response["items"][0]["snippet"]["title"]
        logger.info(f"Video Title: {video_title}")

        # Fetch channel name from cache or API
        if channel_id in channel_metadata_cache:
            channel_name = channel_metadata_cache[channel_id]
            logger.debug(
                f"Channel name for channel ID {channel_id} fetched from cache.")
        else:
            def channel_request_func(youtube_service):
                return youtube_service.channels().list(
                    part="snippet",
                    id=channel_id
                )

            channel_response, youtube_service = retry_request(
                channel_request_func, youtube)
            if channel_response and channel_response.get("items"):
                channel_name = channel_response["items"][0]["snippet"]["title"]
                channel_metadata_cache[channel_id] = channel_name
                logger.info(f"Channel Name: {channel_name}")
            else:
                logger.warning(
                    f"Could not retrieve channel name for channel ID {channel_id}.")
                channel_name = None

        # Cache the video metadata
        video_metadata_cache[video_id] = (video_title, channel_name)

        return video_title, channel_name

    except Exception as e:
        logger.error(
            f"Failed to fetch video metadata for video ID {video_id} and channel ID {channel_id}: {e}"
        )
        return None, None

def get_most_recent_comment_date(db, channel_id, video_id, initial_fetch_date) -> datetime:
    """
    Fetches the most recent comment date for a specific video on a given channel. If no
    comments are found, the function returns the initial fetch date.

    Args:
        db: Database connection object used for fetching the data.
        channel_id: Unique identifier of the channel to query comments from.
        video_id: Unique identifier of the video to query comments for.
        initial_fetch_date: Fallback date to return when no comments are available.

    Returns:
        datetime: The most recent comment date or the initial fetch date.
    """
    most_recent_comment = db.get_most_recent_comment(channel_id, video_id)
    return (
        parse(most_recent_comment["updated_at"])
        if most_recent_comment
        else parse(initial_fetch_date)
    )


def fetch_comments_page(youtube, video_id, page_token, max_results):
    """
    Fetches a single page of comments from a YouTube video using the YouTube Data API.

    This function retrieves a list of comments along with a token pointing to the next
    page of results, if available. The API request is paginated, allowing sequential calls
    to fetch complete video comments. It leverages retry functionality to handle transient
    API failures.

    Args:
        youtube: Object representing an authenticated YouTube API client.
        video_id: The unique identifier of the video for which comments are to be fetched.
        page_token: The token indicating the starting point for fetching the current page
            of comments. Set to None for the first page.
        max_results: The maximum number of comments to fetch in a single API request.

    Returns:
        Tuple:
            - List of dictionaries representing fetched comments. Each dictionary contains
              comment metadata and content.
            - String representing the next page token for subsequent API requests. Returns
              None if no additional pages remain.
            - Updated YouTube API client object, which might include state changes due to
              retry handling.

    Raises:
        Logs an error message when an exception occurs and ensures the function returns an
        empty comments list, None as the next page token, and the YouTube API client object
        in such cases.
    """
    try:
        def request_func(youtube_service):
            return youtube_service.commentThreads().list(
                part="snippet",
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


def save_progress(db, video_id, page_token):
    """
    Saves the current progress for a video by updating its state in the database.

    This function logs a warning if the `video_id` is not provided and skips
    saving the progress. Otherwise, it updates the progress for the provided
    `video_id` with the given `page_token`.

    Args:
        db: Database object responsible for storing and managing video progress.
        video_id: Unique identifier for the video whose progress is being saved.
        page_token: Token indicating the specific page or position to be saved
            for the video.

    """
    if not video_id:
        db.logger.warning(
            "Attempted to save progress without a valid video_id. Skipping.")
        return
    db.save_progress(video_id, page_token)



def get_progress(db, video_id):
    """
    Retrieves the progress of a video from the database.

    This function fetches the progress of a video identified by its video ID from
    the provided database object. It is commonly used to track the viewing status
    or progress of a specific video for a user.

    Args:
        db: The database object from which the progress data is fetched.
        video_id: The unique identifier of the video whose progress is to be
            retrieved.

    Returns:
        The progress of the video as stored in the database.

    """
    return db.get_progress(video_id)


def fetch_comments_with_resume(youtube, video_id, channel_id, db, max_results=100, initial_fetch_date=CUTOFF_DATE, ignore_progress=True):
    """
    Fetches comments for a specified YouTube video by iteratively requesting
    comments data using the YouTube Data API, with a mechanism to resume progress
    using saved page tokens and filtering out older comments based on the most
    recent comment date.

    This function retrieves and enriches new comments, determines whether to
    continue fetching based on the presence of a next page token, and stores
    progress as it fetches.

    Args:
        youtube: An instance of the YouTube API client for performing API calls.
        video_id: The ID of the YouTube video for which to fetch comments.
        channel_id: The ID of the channel to which the video belongs.
        db: A database handle used to store progress and retrieve metadata
            or saved state.
        max_results: The maximum number of comments to fetch per API page call.
            Defaults to 100.
        initial_fetch_date: A cutoff date marking the earliest timestamp for
            comments to include. Defaults to `CUTOFF_DATE`.
        ignore_progress: A boolean indicating whether to ignore progress tracking
            and start fetching comments from the beginning. Defaults to True.

    Returns:
        dict: A dictionary containing the following keys:
            - "video_title": The title of the video.
            - "channel_name": The name of the channel to which the video belongs.
            - "comments": A list of fetched and enriched comments.
            - "youtube_service": The YouTube API client used for fetching data.

    Raises:
        Any API exceptions or database interaction errors will propagate from
        helper functions called within.
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
            "youtube_service": youtube
        }

    # Use progress token only if not ignoring progress
    page_token = None if ignore_progress else get_progress(db, video_id)
    previous_page_token = None  # For safety check
    iteration = 0  # Optional iteration counter

    most_recent_comment_date = get_most_recent_comment_date(
        db, channel_id, video_id, initial_fetch_date)

    while True:
        iteration += 1
        # Safety check: if the page token hasn't changed, break to avoid infinite loop
        if page_token == previous_page_token:
            logger.warning(
                "Page token unchanged from the previous iteration; breaking out to avoid an infinite loop.")
            break
        previous_page_token = page_token

        fetched_comments, next_page_token, youtube = fetch_comments_page(
            youtube, video_id, page_token, max_results)
        if not fetched_comments:
            break

        new_comments_in_page = []
        for comment in fetched_comments:
            comment_date_str = comment["snippet"]["topLevelComment"]["snippet"]["updatedAt"]
            comment_date = parse(comment_date_str)
            if comment_date > most_recent_comment_date:
                enriched_comment = {
                    "video_id": video_id,
                    "video_title": video_title,
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "comment_id": comment["id"],
                    "author": comment["snippet"]["topLevelComment"]["snippet"].get("authorDisplayName"),
                    "author_channel_id": comment["snippet"]["topLevelComment"]["snippet"].get("authorChannelId", {}).get("value"),
                    "text": comment["snippet"]["topLevelComment"]["snippet"].get("textDisplay"),
                    "like_count": comment["snippet"]["topLevelComment"]["snippet"].get("likeCount"),
                    "published_at": comment["snippet"]["topLevelComment"]["snippet"].get("publishedAt"),
                    "updated_at": comment_date_str
                }
                new_comments_in_page.append(enriched_comment)
            else:
                logger.info(
                    f"Skipping older comment {comment['id']} with date {comment_date_str}")

        if new_comments_in_page:
            all_comments.extend(new_comments_in_page)
        else:
            logger.info(
                f"No new comments found in current page for video ID: {video_id}. Ending fetch.")
            if page_token:
                save_progress(db, video_id, page_token)
            break

        if next_page_token:
            save_progress(db, video_id, next_page_token)
            page_token = next_page_token
        else:
            break

    logger.info(
        f"Finished fetching comments for video ID: {video_id}. Total new comments: {len(all_comments)}")
    return {
        "video_title": video_title,
        "channel_name": channel_name,
        "comments": all_comments,
        "youtube_service": youtube
    }




def get_top_channels(youtube, channels=CHANNELS, n=3):
    """
    Fetches the top N channels based on their subscriber count.

    This function iterates over the provided channel information and fetches their
    subscriber count using the YouTube API. The channels are sorted by subscriber
    count in descending order, and the top N channels are returned.

    Args:
        youtube: The YouTube API client used to fetch channel information.
        channels (dict): A dictionary mapping channel names to their information,
            including their handle required for the YouTube API.
        n (int): The number of top channels to return.

    Returns:
        list: A list of tuples representing the top N channels, each tuple contains
            the channel name and its updated information.
    """
    for channel_name, channel_info in channels.items():
        try:

            def request_func(youtube_service):
                return youtube_service.channels().list(
                    part="statistics",
                    forHandle=channel_info.get("handle")
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
                    f"Could not retrieve subscriber count for channel handle: {channel_info.get('handle')}"
                )
                channel_info["subscriber_count"] = 0
        except Exception as e:
            logger.error(
                f"Error fetching subscriber count for channel handle {channel_info.get('handle')}: {e}"
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
    Fetches all comments across all videos associated with a specific YouTube channel
    based on the provided channel ID. This function retrieves comments using the
    YouTube Data API, enriches them with additional metadata, and stores them in a
    database.

    Args:
        youtube: An instance of the authenticated YouTube API client.
        channel_id: A string representing the unique identifier of the YouTube channel.
        db: A database instance capable of inserting comments in bulk.
        max_results: An optional integer specifying the maximum number of comments
            to retrieve per API request. Defaults to 100.
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
        if not channel_response.get("items"):
            logger.warning(f"No channel found with ID {channel_id}.")
            return
        channel_name = channel_response["items"][0]["snippet"]["title"]
        logger.info(f"Channel Name: {channel_name}")
    except Exception as e:
        logger.error(
            f"Failed to fetch channel name for channel ID {channel_id}: {e}"
        )
        channel_name = None

    if not channel_name:
        logger.warning(
            f"Skipping comment fetching for channel ID {channel_id} due to missing channel name."
        )
        return

    page_token = None
    all_new_comments = 0  # To track the total number of new comments fetched

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
            if not comments:
                logger.debug("No comments found on this page.")
                break

            enriched_comments = []

            for comment in comments:
                # Extract necessary fields from the comment thread
                comment_data = comment["snippet"]["topLevelComment"]["snippet"]
                # Changed from "videoId" to "video_id"
                video_id = comment_data.get("videoId")

                if not video_id:
                    logger.warning(
                        "No video ID found in comment thread. Skipping.")
                    continue

                # Fetch video title if not already cached
                video_title, _ = fetch_video_metadata(
                    youtube_service, video_id, channel_id)

                # Enrich comment with standardized structure
                enriched_comment = {
                    "video_id": video_id,
                    "video_title": video_title,
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "comment_id": comment["id"],
                    "author": comment_data.get("authorDisplayName"),
                    "author_channel_id": comment_data.get("authorChannelId"),
                    "text": comment_data.get("textOriginal"),
                    "like_count": comment_data.get("likeCount"),
                    "published_at": comment_data.get("publishedAt"),
                    "updated_at": comment_data.get("updatedAt")
                }

                enriched_comments.append(enriched_comment)

            # Insert all new comments in bulk
            if enriched_comments:
                db.insert_comments(enriched_comments)


            page_token = response.get('nextPageToken')
            if not page_token:
                break

        except HttpError as e:
            logger.error(
                f"An error occurred processing channel {channel_id}: {e}"
            )
            break
        except Exception as e:
            logger.error(
                f"An unexpected error occurred while fetching comments for channel {channel_id}: {e}"
            )
            break

    logger.info(
        f"Finished fetching all comments for channel ID: {channel_id}."
    )


# -----------------By Videos-----------------


def generate_videos_by_search(youtube, channel_id, search_keyword, max_results=50):
    """
    Generates video IDs from a YouTube channel by searching for a specified keyword.

    This function searches for videos in a specific YouTube channel based on the provided
    search keyword and yields the video ID along with the YouTube service instance for
    each video found. The search is performed iteratively, fetching results page by page
    until no more pages are available or an error occurs.

    Args:
        youtube: The YouTube API client used to make API calls.
        channel_id: The ID of the YouTube channel where the search should be conducted.
        search_keyword: The keyword used to filter the search results for videos.
        max_results: The maximum number of results to return per page. The default is 50.

    Yields:
        tuple: A tuple consisting of the video ID and the YouTube service instance for each
        video retrieved through the search.

    Raises:
        HttpError: If an error occurs during the API request.
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
                yield video_id, youtube_service

            page_token = response.get('nextPageToken')
            if not page_token:
                break

        except HttpError as e:
            logger.error(
                f"Error searching videos for keyword '{search_keyword}' in channel {channel_id}: {e}"
            )
            break


def generate_playlists(youtube, channel_id, keywords=KEYWORDS, max_results=10):
    """
    Generates playlists matching specified keywords from a YouTube channel.

    This function retrieves playlists from a specified YouTube channel and filters the playlists
    based on provided keywords. It fetches playlists using the YouTube Data API, paginates through
    the results if necessary, and yields the matching playlist IDs and titles.

    Args:
        youtube: Authorized Google API client for accessing YouTube Data API.
        channel_id: str. The ID of the YouTube channel to retrieve playlists from.
        keywords: list[str]. Keywords to match against playlist titles. Defaults to KEYWORDS.
        max_results: int. Maximum number of playlists to request per API call. Defaults to 10.

    Yields:
        tuple[str, str]: A tuple containing the playlist ID and playlist title for each playlist
        whose title matches any of the specified keywords.
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


def generate_videos(youtube, playlist_id, max_results=50):
    """
    Fetches and yields video IDs from a YouTube playlist.

    This function retrieves videos from a specified YouTube playlist by
    iterating through pages of results via the YouTube Data API. It handles
    pagination and retries the request if the initial request fails. Video IDs
    are extracted from the response and yielded one by one.

    Args:
        youtube: The YouTube client service used to make requests to the
            YouTube Data API.
        playlist_id: The playlist ID from which video IDs are to be retrieved.
        max_results: The maximum number of results to retrieve per request.
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
    Fetches comments from YouTube videos by processing playlists or video search results associated with a given
    channel. If playlists matching specific keywords are found, it processes them to retrieve comments from videos within
    the playlists. If no playlists are found, comments are fetched directly from videos matching the keywords through
    YouTube's search API. All fetched comments are inserted into the provided database.

    Args:
        youtube: The YouTube service instance used for authorized API calls.
        channel_id: The unique identifier of the YouTube channel being processed.
        db: The database instance where fetched comments are stored.
        keywords: A list of keywords to match playlists or videos for comment retrieval. If not a list, it is
            automatically converted to one.
        max_results: The maximum number of results (videos or playlists) to fetch for each API call.
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
                for video_id, youtube_service in videos:
                    if video_id in processed_videos:
                        logger.debug(
                            f"Already processed video ID: {video_id}. Skipping."
                        )
                        continue
                    processed_videos.add(video_id)

                    logger.info(f"Fetching comments for video: {video_id}")

                    result = fetch_comments_with_resume(
                        youtube_service,
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
            try:
                videos_generator = generate_videos(
                    youtube, playlist_id, max_results=100)
                for video_id in videos_generator:
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
                    if result and result["comments"]:
                        db.insert_comments(result["comments"])
                    # Update youtube_service if rotated
                    youtube = result.get("youtube_service", youtube)
            except Exception as e:
                logger.error(
                    f"An error occurred while processing playlist {playlist_id}: {e}"
                )

    except HttpError as e:
        logger.error(
            f"An error occurred processing channel {channel_id} with keywords '{keywords}': {e}"
        )


# -----------------Main Execution-----------------


def process_channels(youtube, db, channels=CHANNELS, keywords=KEYWORDS, limit_channels=None):
    """
    Processes a list of YouTube channels to fetch comments based on various criteria, such as
    playlist or entire channel, and filters comments using specified keywords. Can be configured
    to process only a limited number of top channels sorted by subscriber count.

    Args:
        youtube: An instance of the YouTube API client to interact with YouTube data.
        db: A database connection object for storing and retrieving YouTube comments.
        channels: A dictionary of channels to process with channel information such as IDs
            and processing preferences. If not provided, a default set of channels is
            used.
        keywords: A list of keywords to filter the fetched comments. If not provided,
            a default keyword list is used.
        limit_channels: Optional; the maximum number of top channels (by subscriber count)
            to process. If not provided, all channels will be processed.

    Raises:
        QuotaExhaustedError: Raised when the YouTube API quota is exhausted during execution,
            stopping further processing.
    """
    if not isinstance(keywords, list):
        keywords = list(keywords)
    if limit_channels:
        # Limit the channels to process based on the top subscriber count
        top_channels = get_top_channels(youtube, channels, n=limit_channels)
        channels = dict(top_channels)
        logger.info(f"Fetching comments for channels: {list(channels.keys())}")

    try:
        for channel_name, channel_info in channels.items():
            channel_id = channel_info.get("channel_id")
            if not channel_id:
                logger.warning(f"No channel_id found for channel '{channel_name}'. Skipping.")
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
                    youtube, channel_id, db, keywords, max_results=100)
    except QuotaExhaustedError as e:
        logger.error(f"Quota exhausted: {e}. Stopping further processing.")
        return


def main():
    """
    The main entry point for the script. Sets up the YouTube service, manages
    a database connection, and processes channel data.

    This function initializes the required YouTube service, establishes a connection
    to the database using the context manager, and handles channel data processing.
    It ensures proper closure of the logging system upon script completion.

    Args:
        None

    Returns:
        None
    """
    youtube_service = build_youtube_service()
    with DatabaseConnection() as db:
        process_channels(youtube_service, db)
    logging.shutdown()


if __name__ == "__main__":
    main()
