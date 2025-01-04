import os
import time
import json
from dateutil.parser import parse
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import logging
import logging.config

# config variables and database connection
from config import API_KEY, CHANNELS, CUTOFF_DATE, KEYWORDS
from database_con import DatabaseConnection

# Initialize logger at the module level
logger = logging.getLogger(__name__)


def setup_logging(config_path='logging_config.json'):
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)

    with open(config_path, 'rt') as f:
        config = json.load(f)

    logging.config.dictConfig(config)


def build_youtube_service(api_key=API_KEY):
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
    Raises:
        HttpError: If the request fails due to server-side errors (status codes 500, 502, 503, 504).
        Exception: For any other general exceptions encountered during the request execution.
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


def fetch_comments(youtube, video_id, channel_id, max_results=100, initial_fetch_date=CUTOFF_DATE):
    """
    Fetches comments for a given YouTube video.
    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        video_id (str): The ID of the YouTube video to fetch comments for.
        channel_id (str): The ID of the YouTube channel that owns the video.
        max_results (int, optional): The maximum number of comments to fetch per API call. Defaults to 100.
        initial_fetch_date (str, optional): The initial date to start fetching comments from if no comments are found in the database. Defaults to CUTOFF_DATE.
    Returns:
        list: A list of fetched comments, each represented as a dictionary.
    Raises:
        googleapiclient.errors.HttpError: If an error occurs while making the API request.
    """

    logger.info(f"Fetching comments for video ID: {video_id}")
    all_comments = []

    db_connection = DatabaseConnection()
    with db_connection as db:
        most_recent_comment = db.get_most_recent_comment(channel_id, video_id)
        most_recent_comment_date = (
            parse(most_recent_comment["updatedAt"])
            if most_recent_comment else
            parse(initial_fetch_date)
        )

    page_token = None
    while True:
        try:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id,
                maxResults=max_results,
                order="time",
                pageToken=page_token
            )
            response = retry_request(request)
            if not response:
                break

            fetched_comments = response.get("items", [])
            if not fetched_comments:
                break

            for comment in fetched_comments:
                comment_date = parse(
                    comment["snippet"]["topLevelComment"]["snippet"]["updatedAt"]
                )
                if comment_date <= most_recent_comment_date:
                    # Reached already-fetched or older comments
                    return all_comments
                all_comments.append(comment)

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        except HttpError as e:
            logger.error(
                f"An error occurred fetching comments for video {video_id}: {e}")
            break

    logger.info(
        f"Finished fetching comments for video ID: {video_id}. Total new comments: {len(all_comments)}")
    return all_comments


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
        request = youtube.channels().list(
            part="statistics",
            forHandle=channel_info["handle"]
        )
        response = retry_request(request)
        if response and "items" in response and response["items"]:
            subscriber_count = response["items"][0]["statistics"]["subscriberCount"]
            channel_info["subscriber_count"] = subscriber_count
        else:
            logger.warning(
                f"Could not retrieve subscriber count for channel handle: {channel_info['handle']}")

    # Sort the channels by subscriber count
    sorted_channels = sorted(
        channels.items(),
        key=lambda x: int(x[1].get("subscriber_count", 0)),
        reverse=True
    )
    return sorted_channels[:n]


# -----------------By Channel-----------------

def get_all_channel_comments(youtube, channel_id, max_results=100):
    """
    Fetches all comment threads for a given YouTube channel ID and stores them in a database.
    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        channel_id (str): The ID of the YouTube channel to fetch comments from.
        max_results (int, optional): The maximum number of results to retrieve per API call. Defaults to 100.
    Returns:
        None
    Raises:
        googleapiclient.errors.HttpError: If an error occurs while making the API request.
    Logs:
        Info: When fetching comment threads for the specified channel ID.
        Error: If an error occurs while processing the channel.
    """

    logger.info(f"Fetching all comment threads for channel ID: {channel_id}")
    page_token = None

    while True:
        try:
            request = youtube.commentThreads().list(
                part="snippet,replies",
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
                with DatabaseConnection() as db:
                    # Insert each comment individually or via insert_comments
                    db.insert_comments(json.loads(json.dumps(comments)))

            page_token = response.get('nextPageToken')
            if not page_token:
                break

        except HttpError as e:
            logger.error(
                f"An error occurred processing channel {channel_id}: {e}")
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
        keywords = [keywords]

    page_token = None
    while True:
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


def get_comments_by_playlist(youtube, channel_id, keywords=KEYWORDS, max_results=5):
    """
    Fetches comments from YouTube videos in playlists that match specified keywords.
    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        channel_id (str): The ID of the YouTube channel to search for playlists.
        keywords (list or str, optional): A list of keywords to filter playlists. Defaults to KEYWORDS.
        max_results (int, optional): The maximum number of playlists to retrieve. Defaults to 5.
    Raises:
        googleapiclient.errors.HttpError: If an error occurs while making API requests.
    Returns:
        None
    """

    if not isinstance(keywords, list):
        keywords = [keywords]

    try:
        playlists = generate_playlists(
            youtube, channel_id, keywords, max_results=max_results
        )
        for playlist_id, playlist_title in playlists:
            print(f"Playlist ID: {playlist_id}, Title: {playlist_title}")
            videos = generate_videos(youtube, playlist_id)
            for video_id in videos:
                print(f"Fetching comments for video: {video_id}")
                comments = fetch_comments(youtube, video_id, channel_id)
                if comments:
                    with DatabaseConnection() as db:
                        db.insert_comments(comments)
    except HttpError as e:
        logger.error(
            f"An error occurred processing channel {channel_id} with keywords '{keywords}': {e}"
        )


# -----------------By Videos-----------------

def filter_videos(youtube, channel_id, keywords=KEYWORDS, max_results=20):
    """
    Filters videos from a YouTube channel based on specified keywords and fetches comments for each video.
    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        channel_id (str): The ID of the YouTube channel to filter videos from.
        keywords (list or str, optional): A list of keywords or a single keyword to filter videos by. Defaults to KEYWORDS.
        max_results (int, optional): The maximum number of results to return per page. Defaults to 20.
    Returns:
        None
    """

    if not isinstance(keywords, list):
        keywords = [keywords]

    for keyword in keywords:
        logger.info(
            f"Filtering videos for channel {channel_id} by keyword '{keyword}'")
        page_token = None

        while True:
            try:
                request = youtube.search().list(
                    part="snippet",
                    channelId=channel_id,
                    q=keyword,  # Use each keyword here
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
                    print(f"Fetching comments for video: {video_id}")
                    comments = fetch_comments(youtube, video_id, channel_id)
                    if comments:
                        with DatabaseConnection() as db:
                            db.insert_comments(comments)

                page_token = response.get('nextPageToken')
                if not page_token:
                    break

            except HttpError as e:
                logger.error(
                    f"An error occurred processing channel {channel_id} with keyword '{keyword}': {e}"
                )
                break


def process_channels(youtube, channels=CHANNELS, keywords=KEYWORDS, limit_channels=None):
    """
    Processes YouTube channels to fetch comments based on specified keywords.
    Args:
        youtube (googleapiclient.discovery.Resource): The YouTube API client.
        channels (list): A list of channels to process. Defaults to CHANNELS.
        keywords (list or str): A list of keywords to filter comments. Defaults to KEYWORDS.
        limit_channels (int, optional): The number of top channels to process based on subscriber count. Defaults to None.
    Returns:
        None
        """
    if not isinstance(keywords, list):
        keywords = [keywords]
    if limit_channels:
        # Limit the channels to process based on the top subscriber count
        top_channels = get_top_channels(youtube, channels, n=limit_channels)
        channels = dict(top_channels)

    for channel in channels:
        if channel["only_wow"]:
            get_all_channel_comments(youtube, channel["channel_id"])
        else:
            # For "only_wow" = False, fetch by playlist using multiple keywords
            get_comments_by_playlist(youtube, channel["channel_id"], keywords)


if __name__ == "__main__":
    setup_logging()
    logger.info("Application started and logging is configured.")
    youtube = build_youtube_service()

    # Example usage:
    # Auto process all channels based on the configuration
    process_channels(youtube)
    # or:
    # get all comments from a specific channel
    get_all_channel_comments(
        youtube, channel_id=CHANNELS['World of Warcraft']['channel_id'])
    # or:
    # filter videos by keywords from channels and keywords
    # filter_videos(youtube, channel_id=CHANNELS['World of Warcraft']['channel_id']", keywords=KEYWORDS)
    # or:
    # get comments by playlist from a specific channel and playlist keywords
    # get_comments_by_playlist(youtube, channel_id=CHANNELS['Bellular Warcraft']['channel_id'], keywords=["Warcraft Lore", "Warcraft News"])
