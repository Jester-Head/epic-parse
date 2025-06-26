# File: arguments.py
import argparse


def parse_cli() -> argparse.Namespace:
    """
    Parses command-line arguments for a YouTube comment scraper that allows flexible filtering and
    selection of YouTube channels based on various criteria.

    This function creates and configures an instance of `ArgumentParser` to provide several
    options for filtering channels, such as inclusion by name, exclusion by name, filtering by channel
    tags, and limiting based on subscriber count or inactivity. Additionally, it includes options to
    validate the health of channels.

    Returns:
        Namespace: The parsed arguments as a namespace object, containing all the command-line
        options and their respective values. The namespace provides access to user input for filtering
        and processing YouTube channels.
    """
    p = argparse.ArgumentParser(
        description="YouTube comment scraper with flexible channel selection"
    )
    p.add_argument(
        "--channels",
        metavar="NAME1,NAME2",
        help="Comma-separated list of channel names to include (keys in CHANNELS).",
    )
    p.add_argument(
        "--skip",
        metavar="NAME1,NAME2",
        help="Comma-separated list of channel names to exclude.",
    )
    p.add_argument(
        "--types",
        metavar="TAG1,TAG2",
        help="Include only channels whose `tags` contain ANY of these labels.",
    )
    p.add_argument(
        "--exclude-types",
        metavar="TAG1,TAG2",
        help="Exclude channels whose `tags` contain ANY of these labels "
             "(applied after --types / --channels).",
    )
    p.add_argument(
        "--limit-top",
        type=int,
        metavar="N",
        help="Keep only the top-N channels by subscriber count "
             "(after previous filters, before --skip / --exclude-types).",
    )
    p.add_argument(
        "--limit-bottom",
        type=int,
        metavar="N",
        help="Keep only the bottom-N channels by subscriber count (after previous filters, before --skip / --exclude-types).",
    )
    p.add_argument(
        "--max-inactive-days",
        type=int,
        metavar="DAYS",
        help="Skip any channel whose last upload is older than DAYS.",
    )
    p.add_argument(
        "--verify-channels",
        action="store_true",
        help="Just print a health report for every channel and exit.",
    )
    p.add_argument(
        "--verify-cutoff-days",
        type=int,
        default=365,
        metavar="N",
        help="Channel is flagged inactive if no upload in the last N days "
             "(used with --verify-channels).",
    )

    return p.parse_args()