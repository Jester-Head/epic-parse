#!/usr/bin/env python
"""
run_full_pipeline.py
────────────────────────────────────────────────────────────────────────────
Orchestrates the Reddit ingest pipeline for Epic Parse:

1. (Optional) Bulk-seed history with Pushshift up to N days ago
2. Fill the most-recent slice with the Reddit API (PRAW)
3. (Optional) Start the live stream (reddit_stream.py)

Examples
--------
# Use default 90-day cutoff, skip Pushshift, then start the stream
python run_full_pipeline.py --start-stream

# Run Pushshift for a deeper seed, then PRAW, then the stream
python run_full_pipeline.py --with-pushshift --days 120 --start-stream

# PRAW-only back-fill, no stream
python run_full_pipeline.py --days 60
────────────────────────────────────────────────────────────────────────────
Requires
--------
• reddit_backfill.py  (PRAW-only is fine; Pushshift loader optional)
• reddit_stream.py
• misc_config.py      (Reddit & Mongo creds)
• Env with praw, pymongo, python-dateutil, tqdm   (psaw only if you keep Pushshift)
"""

import argparse
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List  # for < 3.9 compatibility

HERE = Path(__file__).parent.resolve()
PYTHON = sys.executable            # current interpreter


def iso_days_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")


def run(cmd: List[str]) -> None:
    print("→", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError:
        sys.exit(f"✕ Command failed: {' '.join(cmd)}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Epic Parse ingest pipeline.")
    parser.add_argument(
        "--days", type=int, default=90,
        help="How many days Pushshift currently lags (default 90)."
    )
    parser.add_argument(
        "--python-exe", default=PYTHON,
        help="Python executable to use (default: current interpreter)."
    )
    # Pushshift is OFF by default now
    parser.add_argument("--with-pushshift", action="store_true",
                        help="Enable the Pushshift back-fill step.")
    parser.add_argument("--no-praw", action="store_true",
                        help="Skip the PRAW back-fill step.")
    parser.add_argument("--start-stream", action="store_true",
                        help="Launch reddit_stream.py after back-fills "
                             "(keeps this script running).")
    args = parser.parse_args()

    cutoff = iso_days_ago(args.days)

    # Step 1: Pushshift back-fill (optional)
    if args.with_pushshift:
        cmd_ps = [
            args.python_exe, str(HERE / "reddit_backfill.py"),
            "--after", "2005-06-23",
            "--before", cutoff,
        ]
        run(cmd_ps)

    # Step 2: PRAW back-fill (optional)
    if not args.no_praw:
        cmd_praw = [
            args.python_exe, str(HERE / "reddit_backfill.py"),
            "--after", cutoff,
        ]
        run(cmd_praw)

    # Step 3: live stream (optional)
    if args.start_stream:
        cmd_stream = [
            args.python_exe, str(HERE / "reddit_stream.py"),
        ]
        print("→ Launching live stream (Ctrl-C to stop)\n")
        # foreground; passes Ctrl-C through
        subprocess.run(cmd_stream, check=False)


if __name__ == "__main__":
    main()
