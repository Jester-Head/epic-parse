#!/usr/bin/env python
"""
reddit_backfill.py – Historic loader for Epic Parse (PRAW-only).

Examples
--------
# Back-fill from 2023-01-01 to today
python reddit_backfill.py --after 2023-01-01

# Back-fill an explicit range
python reddit_backfill.py --after 2024-06-01 --before 2024-12-31
-----------------------------------------------------------------
Dependencies
------------
pip install praw pymongo python-dateutil tqdm
misc_config.py must define:
    REDDIT_ID, REDDIT_SECRET, REDDIT_USER, REDDIT_PASS, MONGO_URI, MONGO_DB
"""

import time
import argparse
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse as dateparse
from tqdm import tqdm

import pymongo
import praw

from misc_config import (
    REDDIT_ID,
    REDDIT_SECRET,
    REDDIT_USER,
    REDDIT_PASS,
    MONGO_URI,
    MONGO_DB,
)

# ─────────── CONFIG ───────────
SUBREDDITS = ["wow", "classicwow", "CompetitiveWoW", "wownoob"]
DB_NAME = MONGO_DB
COLL_POSTS = "reddit_posts"
PRAW_CHUNK_DAYS = 1  # window size (1 day keeps < 1 000 results)

# ─────────── Mongo ────────────
mongo = pymongo.MongoClient(MONGO_URI)
posts_col = mongo[DB_NAME][COLL_POSTS]

# ─────────── Helpers ──────────


def utc_dt(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def save_post(doc: dict) -> None:
    posts_col.update_one({"_id": doc["_id"]}, {"$set": doc}, upsert=True)

# ───────── PRAW Loader ─────────


def load_praw(start_dt: datetime, end_dt: datetime) -> None:
    reddit = praw.Reddit(
        client_id=REDDIT_ID,
        client_secret=REDDIT_SECRET,
        username=REDDIT_USER,
        password=REDDIT_PASS,
        user_agent=f"windows:epicparse:v1.0 (by u/{REDDIT_USER})",
    )

    step = timedelta(days=PRAW_CHUNK_DAYS)
    total_days = (end_dt - start_dt).days
    with tqdm(total=total_days * len(SUBREDDITS), desc="PRAW days") as bar:
        t0 = start_dt
        while t0 < end_dt:
            t1 = min(t0 + step, end_dt)

            for sub_name in SUBREDDITS:
                subreddit = reddit.subreddit(sub_name)
                query = f"timestamp:{int(t0.timestamp())}..{int(t1.timestamp())}"

                for s in subreddit.search(
                    query,
                    syntax="cloudsearch",
                    sort="new",
                    limit=None
                ):
                    save_post(
                        {
                            "_id":          f"t3_{s.id}",
                            "subreddit":    sub_name,
                            "title":        s.title,
                            "selftext":     s.selftext,
                            "author":       str(s.author),
                            "score":        s.score,
                            "num_comments": s.num_comments,
                            "created_utc":  utc_dt(s.created_utc),
                            "url":          s.url,
                            "flair":        s.link_flair_text,
                            "permalink":    s.permalink,
                        }
                    )

                bar.update(1)   # one tick per subreddit-day

            t0 = t1
            time.sleep(1)       # keeps ≤ ~60 QPM total

# ─────────── CLI ──────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reddit post back-fill (PRAW only).")
    parser.add_argument(
        "--after",
        default="2018-08-14",                    # WoW US launch date
        help="Start date YYYY-MM-DD (default 2018-08-14).",
    )
    parser.add_argument(
        "--before",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="End date YYYY-MM-DD (default today).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_dt = dateparse(args.after).replace(tzinfo=timezone.utc)
    end_dt = dateparse(args.before).replace(tzinfo=timezone.utc)
    if start_dt >= end_dt:
        raise ValueError("--after must be earlier than --before")

    print(f"Back-fill window : {start_dt.date()} → {end_dt.date()}")
    load_praw(start_dt, end_dt)
    print("Posts :", posts_col.count_documents({}))


if __name__ == "__main__":
    main()
