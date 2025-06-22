# File: main.py

import logging
import sys
from pprint import pprint

from api.channels import ChannelManager
from api.comments import CommentManager
from api.metadata import MetadataManager
from api.playlists import PlaylistManager
from api.youtube_client import YouTubeClient
from cli.arguments import parse_cli
from cli.channel_filter import ChannelFilter
from config import CHANNELS
from core.processor import YouTubeProcessor
from database_con import DatabaseConnection
from utils.cache import CacheManager
from utils.logging_setup import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def main() -> None:
    """
    Main script to process YouTube channels with various management and processing
    components. Handles command-line argument parsing, applies filters for channel
    selection, optionally verifies channel activity, and processes selected channels.

    This script uses multiple manager classes to interact with YouTube data,
    enabling functionalities such as metadata handling, comments management, and
    channel processing. Additionally, it integrates caching mechanisms and database
    operations to optimize the workflow.

    The script includes error handling to manage interruptions (e.g., via
    KeyboardInterrupt) or unexpected exceptions gracefully.

    Raises:
        KeyboardInterrupt: If the process is terminated by the user manually.
        Exception: For any unexpected errors during execution, logs the error
            details and exits the program with an error code.
    """
    try:
        args = parse_cli()

        cache_manager = CacheManager()
        youtube_client = YouTubeClient()
        metadata_manager = MetadataManager(cache_manager)
        comment_manager = CommentManager(metadata_manager)
        channel_manager = ChannelManager(metadata_manager, comment_manager)
        playlist_manager = PlaylistManager()

        processor = YouTubeProcessor(
            youtube_client, metadata_manager, comment_manager,
            channel_manager, playlist_manager
        )

        channel_filter = ChannelFilter(channel_manager)

        selected_channels = channel_filter.apply_filters(CHANNELS, args, youtube_client)

        if not selected_channels:
            logger.error("No channels left to process after filtering.")
            return

        # Verify the channel is still active
        if args.verify_channels:
            report = channel_manager.verify_channels(
                youtube_client, selected_channels, cutoff_days=args.verify_cutoff_days
            )

            formatted_report = {
                k: {
                    "exists": v["exists"],
                    "inactive": v["inactive"],
                    "last_upload": (
                        v["last_upload"].date().isoformat() if v["last_upload"] else None
                    ),
                } for k, v in report.items()
            }
            pprint(formatted_report)
            return

        with DatabaseConnection() as db:
            processor.process_channels(db, channels=selected_channels)

    except KeyboardInterrupt:
        logger.info("Process interrupted by user. Exiting gracefully.")
        sys.exit(0)
    except Exception as e:
        logger.error("Unexpected error: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
