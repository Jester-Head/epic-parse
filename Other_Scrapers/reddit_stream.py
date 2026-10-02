"""
reddit_stream.py – real-time posts + comments

Run: python reddit_stream.py
Stop: Ctrl-C
"""

import sys
import os
import time
import signal
import threading
from datetime import datetime, timezone
import praw
import pymongo
from misc_config import (
    REDDIT_ID, REDDIT_SECRET, REDDIT_USER, REDDIT_PASS,
    MONGO_URI, MONGO_DB,
)

# ───────── Config ─────────
SUBREDDITS = ["wow", "classicwow", "CompetitiveWoW", "wownoob"]
SUB_UNION = "+".join(SUBREDDITS)
SLEEP_SEC = 1.2                     # 1.2 s × 2 threads ≈ 100 QPM
DB_NAME = MONGO_DB
COLL_POSTS = "reddit_posts"
COLL_COMMENTS = "reddit_comments"

# ───────── Helpers ────────


def utc_dt(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def shutdown(_sig, _frame):
    print("\nShutting down …")
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown)

# ───────── Clients ────────
reddit = praw.Reddit(
    client_id=REDDIT_ID,
    client_secret=REDDIT_SECRET,
    username=REDDIT_USER,
    password=REDDIT_PASS,
    user_agent=f"windows:epicparse:v1.0 (by u/{REDDIT_USER})",
)

mongo = pymongo.MongoClient(MONGO_URI)
posts_col = mongo[DB_NAME][COLL_POSTS]
comments_col = mongo[DB_NAME][COLL_COMMENTS]

# ───────── Mongo upserts ───


def save_post(s):
    doc = {
        "_id":          f"t3_{s.id}",
        "subreddit":    s.subreddit.display_name,
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
    posts_col.update_one({"_id": doc["_id"]}, {"$set": doc}, upsert=True)


def save_comment(c):
    doc = {
        "_id":           f"t1_{c.id}",
        "submission_id": c.link_id.split("_")[1],
        "parent_id":     c.parent_id,
        "subreddit":     c.subreddit.display_name,
        "body":          c.body,
        "author":        str(c.author),
        "score":         c.score,
        "created_utc":   utc_dt(c.created_utc),
    }
    comments_col.update_one({"_id": doc["_id"]}, {"$set": doc}, upsert=True)

# ───────── Stream workers ──


def post_worker():
    print("Post stream  ->", SUB_UNION)
    for s in reddit.subreddit(SUB_UNION).stream.submissions(skip_existing=True):
        save_post(s)
        time.sleep(SLEEP_SEC)


def comment_worker():
    print("Comment stream ->", SUB_UNION)
    for c in reddit.subreddit(SUB_UNION).stream.comments(skip_existing=True):
        save_comment(c)
        time.sleep(SLEEP_SEC)


# ───────── Main ────────────
if __name__ == "__main__":
    threading.Thread(target=comment_worker, daemon=True).start()
    post_worker()        # foreground
