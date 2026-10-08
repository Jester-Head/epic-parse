"""Reddit: posts and comments from WoW subreddits, through Reddit's OAuth API.

Credentials: REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET and REDDIT_USER_AGENT in .env. Uses
application-only OAuth (no account login) and reads public listings only.

Guardrails:
- Only the WoW subreddits in SUBREDDITS (or ones named with --category).
- Well under the free tier's 100 requests per minute: one request per `delay` seconds (default
  1.5, about 40 a minute), plus a pause whenever Reddit's X-Ratelimit-Remaining header runs low.
- Reddit authors are never linked to forum accounts or WoW characters. Character and account
  matching (raiderio, forum_profiles, people) reads only Blizzard forum posts.
- Posts and comments deleted or removed on Reddit are blanked here as well (check_deletions), and
  Reddit raw pages are deleted after RAW_KEEP_DAYS so deleted text doesn't linger in them.
- Reddit text is for private analysis; it doesn't go into anything published.

Reddit listings stop at about 1,000 items, so the first run only reaches as far back as /new
still goes (a few days on r/wow). Running daily keeps up from then on.

fetch(): each subreddit's /new listing; a thread's comments are fetched when it's new or its
         comment count changed since its comments were last fetched. Then parse(),
         check_deletions() for recent items, and the raw page cleanup.
parse(): listings -> threads; comment pages -> the thread, its opening post and its comments.
"""
import logging
import os
import re
import threading
import time

import httpx
from psycopg.types.json import Jsonb

from epic_parse import wow_patches
from epic_parse.db import source_id
from epic_parse.fetch import Fetcher

log = logging.getLogger(__name__)

SOURCE = "reddit"
SUBREDDITS = [
    # retail
    "wow", "CompetitiveWoW", "wownoob", "woweconomy", "worldofpvp", "Transmogrification", "warcraftlore",
    "WowUI", "WoWHousing", "wowaddons",
    # Classic (tagged game_version 'classic' from the subreddit name)
    "classicwow", "wowclassic", "wowhardcore", "classicwowtbc",
    # WoW Forever
    "wowforever", "classicwowplus",
]
# Subreddits whose name would tag them wrongly: an extra hint for wow_patches.tag_posts, which reads
# the thread's category (the subreddit) plus extra.parent_category.
FORUM_HINTS = {"classicwowplus": "WoW Forever"}
API = "https://oauth.reddit.com"
TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
TOKEN_MAX_AGE = 12 * 3600       # tokens last 24 hours
LISTING_PAGES = 10              # 100 per page; Reddit stops at about 1,000
MORE_CALLS_PER_THREAD = 20      # "load more comments" requests per thread, 100 comments each
DELETION_DAYS = 14              # fetch() re-checks items this recent for deletion
RAW_KEEP_DAYS = 7
GONE = {"[deleted]", "[removed]"}


class Token:
    """Application-only OAuth token."""

    def __init__(self):
        self._lock = threading.Lock()
        self._value: str | None = None
        self._at = 0.0

    def get(self, force: bool = False) -> str:
        with self._lock:
            if force or not self._value or time.monotonic() - self._at > TOKEN_MAX_AGE:
                resp = httpx.post(TOKEN_URL, data={"grant_type": "client_credentials"},
                                  auth=(os.environ["REDDIT_CLIENT_ID"], os.environ["REDDIT_CLIENT_SECRET"]),
                                  headers={"User-Agent": os.environ["REDDIT_USER_AGENT"]}, timeout=30)
                resp.raise_for_status()
                self._value, self._at = resp.json()["access_token"], time.monotonic()
            assert self._value is not None
            return self._value


def _get(fetcher: Fetcher, token: Token, path: str, kind: str, params: dict, transform=None):
    """GET an API path with the token (refreshed once on 401), pausing if the rate limit runs low."""
    params = {**params, "raw_json": 1}
    for attempt in (1, 2):
        fetcher.client.headers["Authorization"] = f"Bearer {token.get(force=attempt == 2)}"
        raw_id, data = fetcher.get_json(API + path, kind, params=params, transform=transform)
        if fetcher.last_status != 401:
            break
    try:
        remaining = float(fetcher.last_headers.get("x-ratelimit-remaining", 100))
        reset = float(fetcher.last_headers.get("x-ratelimit-reset", 0))
    except ValueError:
        remaining, reset = 100, 0
    if remaining < 10:
        log.info("Reddit rate limit nearly used up, waiting %.0fs", reset + 1)
        time.sleep(reset + 1)
    return raw_id, data


def _client(conn, delay: float) -> tuple[Fetcher, Token]:
    fetcher = Fetcher(conn, source_id(conn, SOURCE), delay=delay)
    fetcher.client.headers["User-Agent"] = os.environ["REDDIT_USER_AGENT"]  # Reddit requires its own format
    return fetcher, Token()


def _fetch_thread(fetcher: Fetcher, token: Token, sub: str, post_id: str) -> None:
    """The thread's comment page, then its collapsed "load more" comments."""
    _, data = _get(fetcher, token, f"/r/{sub}/comments/{post_id}", "reddit_comments",
                   {"limit": 500, "sort": "old"}, transform=lambda d: {"listings": d})
    if not data:
        return
    pending = []
    for listing in data["listings"][1:]:
        pending += _more_ids(listing["data"]["children"])
    calls = 0
    while pending and calls < MORE_CALLS_PER_THREAD:
        batch, pending = pending[:100], pending[100:]
        _, more = _get(fetcher, token, "/api/morechildren", "reddit_more",
                       {"api_type": "json", "link_id": f"t3_{post_id}", "children": ",".join(batch), "sort": "old"},
                       transform=lambda d: {"link_id": f"t3_{post_id}", **d})
        calls += 1
        pending += _more_ids((((more or {}).get("json") or {}).get("data") or {}).get("things", []))


def _more_ids(children: list) -> list[str]:
    """Ids behind every "load more comments" stub in a comment tree."""
    ids = []
    for c in children:
        if c.get("kind") == "more":
            ids += [i for i in c["data"].get("children", []) if i != "_"]
        elif c.get("kind") == "t1":
            replies = c["data"].get("replies")
            if isinstance(replies, dict):
                ids += _more_ids(replies["data"]["children"])
    return ids


def fetch(conn, categories: list[str] | None = None, max_pages: int | None = None,
          max_topics: int | None = None, delay: float = 1.5, **_) -> None:
    """Fetch new and changed threads from each subreddit, then parse and check for deletions."""
    fetcher, token = _client(conn, delay)
    src = fetcher.source_id
    try:
        for sub in categories or SUBREDDITS:
            after, seen, fetched = None, 0, 0
            for _page in range(max_pages or LISTING_PAGES):
                _, data = _get(fetcher, token, f"/r/{sub}/new", "reddit_listing", {"limit": 100, "after": after})
                children = ((data or {}).get("data") or {}).get("children", [])
                for child in children:
                    d = child["data"]
                    seen += 1
                    row = conn.execute(
                        "SELECT (extra->>'comments_fetched')::int FROM threads WHERE source_id = %s AND source_key = %s",
                        (src, d["id"]),
                    ).fetchone()
                    if row is not None and row[0] == d.get("num_comments"):
                        continue
                    if max_topics is not None and fetched >= max_topics:
                        continue
                    try:
                        _fetch_thread(fetcher, token, sub, d["id"])
                        fetched += 1
                    except Exception as e:  # one bad thread shouldn't stop the run
                        log.warning("r/%s thread %s failed: %s", sub, d["id"], e)
                after = ((data or {}).get("data") or {}).get("after")
                if not after or not children:
                    break
            log.info("r/%s: %d threads listed, %d new or changed fetched", sub, seen, fetched)
    finally:
        fetcher.close()
    parse(conn)
    check_deletions(conn, days=DELETION_DAYS, delay=delay)
    purge_raw(conn)


MD_LINK = re.compile(r"\[([^\]]*)\]\([^)]*\)")
MD_MARKS = re.compile(r"(\*\*|__|~~|>!|!<|&nbsp;|^#+\s*)", re.M)


def _plain(text: str) -> str:
    """Markdown to plain text: links become their text; bold, strike, spoiler and heading marks go."""
    return MD_MARKS.sub("", MD_LINK.sub(r"\1", text))


def _split_quotes(text: str | None) -> tuple[str | None, list[str]]:
    """Separate Reddit's "> quoted" lines from the author's own text, as plain text."""
    if not text:
        return None, []
    text = _plain(text)
    own, quotes = [], []
    for line in text.splitlines():
        if line.lstrip().startswith(">"):
            quotes.append(re.sub(r"^\s*>\s?", "", line))
        else:
            own.append(line)
    body = "\n".join(own).strip()
    return body or None, [q for q in quotes if q.strip()]


def _compact(d: dict) -> dict:
    return {k: v for k, v in d.items() if v not in (None, "", [], {})}


def _upsert_thread(conn, src: int, d: dict, comments_fetched: bool = False) -> int:
    author = d.get("author") if d.get("author") not in GONE else None
    extra = _compact({
        "parent_category": FORUM_HINTS.get((d.get("subreddit") or "").lower()),
        "score": d.get("score"),
        "upvote_ratio": d.get("upvote_ratio"),
        "num_comments": d.get("num_comments"),
        "comments_fetched": d.get("num_comments") if comments_fetched else None,
        "flair": d.get("link_flair_text"),
        "link": None if d.get("is_self") else d.get("url"),
        "domain": None if d.get("is_self") else d.get("domain"),
        "nsfw": d.get("over_18") or None,
        "stickied": d.get("stickied") or None,
        "locked": d.get("locked") or None,
        "deleted": (d.get("selftext") in GONE or d.get("author") in GONE) or None,
    })
    return conn.execute(
        """
        INSERT INTO threads (source_id, source_key, category, title, author, url, created_at, post_count, extra)
        VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s), %s, %s)
        ON CONFLICT (source_id, source_key) DO UPDATE SET
            category = EXCLUDED.category, title = EXCLUDED.title, author = EXCLUDED.author,
            url = EXCLUDED.url, post_count = EXCLUDED.post_count,
            extra = threads.extra || EXCLUDED.extra, updated_at = now()
        RETURNING id
        """,
        (src, d["id"], d.get("subreddit"), d.get("title"), author, "https://www.reddit.com" + d.get("permalink", ""),
         d.get("created_utc"), (d.get("num_comments") or 0) + 1, Jsonb(extra)),
    ).fetchone()[0]


def _upsert_post(conn, src: int, thread_id: int, raw_id: int, key: str, d: dict, text: str | None,
                 position: int | None) -> None:
    gone = text in GONE or d.get("author") in GONE
    body, quotes = (None, []) if gone else _split_quotes(text)
    extra = _compact({
        "parent": d.get("parent_id"),
        "score": d.get("score"),
        "flair": d.get("author_flair_text") if not gone else None,
        "edited": bool(d.get("edited")) or None,
        "stickied": d.get("stickied") or None,
        "quotes": quotes,
        "deleted": gone or None,
    })
    conn.execute(
        """
        INSERT INTO posts (source_id, source_key, thread_id, position, author, body, created_at, updated_at,
                           raw_page_id, extra)
        VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s), %s, %s)
        ON CONFLICT (source_id, source_key) DO UPDATE SET
            thread_id = EXCLUDED.thread_id, position = EXCLUDED.position, author = EXCLUDED.author,
            body = EXCLUDED.body, updated_at = EXCLUDED.updated_at, raw_page_id = EXCLUDED.raw_page_id,
            extra = EXCLUDED.extra  -- parse() re-tags game version / patch afterwards
        """,
        (src, key, thread_id, position, None if gone else d.get("author"), body, d.get("created_utc"),
         d.get("edited") if isinstance(d.get("edited"), (int, float)) and d.get("edited") else None,
         raw_id, Jsonb(extra)),
    )


def _upsert_comments(conn, src: int, thread_id: int, raw_id: int, children: list) -> int:
    n = 0
    for c in children:
        if c.get("kind") != "t1":
            continue
        d = c["data"]
        _upsert_post(conn, src, thread_id, raw_id, d["name"], d, d.get("body"), None)
        n += 1
        replies = d.get("replies")
        if isinstance(replies, dict):
            n += _upsert_comments(conn, src, thread_id, raw_id, replies["data"]["children"])
    return n


def parse(conn, batch_size: int = 100) -> None:
    """Turn unparsed Reddit raw pages into threads and posts."""
    src = source_id(conn, SOURCE)
    pages = threads = posts = 0
    while True:
        with conn.transaction():
            batch = conn.execute(
                "SELECT id, kind, body FROM raw_pages WHERE source_id = %s "
                "AND kind IN ('reddit_listing', 'reddit_comments', 'reddit_more') "
                "AND status = 200 AND parsed_at IS NULL ORDER BY id LIMIT %s",
                (src, batch_size),
            ).fetchall()
            if not batch:
                break
            for raw_id, kind, body in batch:
                if kind == "reddit_listing":
                    for child in body["data"]["children"]:
                        _upsert_thread(conn, src, child["data"])
                        threads += 1
                elif kind == "reddit_comments":
                    post_listing, *comment_listings = body["listings"]
                    d = post_listing["data"]["children"][0]["data"]
                    thread_id = _upsert_thread(conn, src, d, comments_fetched=True)
                    threads += 1
                    _upsert_post(conn, src, thread_id, raw_id, d["name"], d, d.get("selftext") or None, 1)
                    posts += 1
                    for listing in comment_listings:
                        posts += _upsert_comments(conn, src, thread_id, raw_id, listing["data"]["children"])
                else:  # reddit_more: a flat list of comments from one thread
                    row = conn.execute("SELECT id FROM threads WHERE source_id = %s AND source_key = %s",
                                       (src, body["link_id"][3:])).fetchone()
                    things = (((body.get("json") or {}).get("data") or {}).get("things")) or []
                    if row:
                        posts += _upsert_comments(conn, src, row[0], raw_id, things)
                conn.execute("UPDATE raw_pages SET parsed_at = now() WHERE id = %s", (raw_id,))
            pages += len(batch)

    with conn.transaction():
        linked = conn.execute(
            """
            UPDATE posts c SET parent_id = p.id
            FROM posts p
            WHERE c.source_id = %(src)s AND c.parent_id IS NULL AND c.extra ? 'parent'
              AND p.source_id = %(src)s AND p.source_key = c.extra->>'parent'
            """,
            {"src": src},
        ).rowcount
    log.info("Parsed %d Reddit raw pages: %d thread updates, %d post upserts, %d replies linked",
             pages, threads, posts, linked)
    wow_patches.tag_posts(conn, SOURCE)


def check_deletions(conn, days: int | None = DELETION_DAYS, delay: float = 1.5) -> None:
    """Blank posts and comments that were deleted or removed on Reddit (`days` None = everything)."""
    fetcher, token = _client(conn, delay)
    src = fetcher.source_id
    keys = [r[0] for r in conn.execute(
        """SELECT source_key FROM posts WHERE source_id = %s AND (body IS NOT NULL OR author IS NOT NULL)
           AND (%s::int IS NULL OR created_at > now() - make_interval(days => %s::int))""",
        (src, days, days),
    )]
    gone = []
    try:
        for i in range(0, len(keys), 100):
            batch = keys[i:i + 100]
            _, data = _get(fetcher, token, "/api/info", "reddit_info", {"id": ",".join(batch)})
            if data is None:
                continue
            found = {}
            for c in data["data"]["children"]:
                d = c["data"]
                found[d["name"]] = d.get("body", d.get("selftext")) in GONE or d.get("author") in GONE
            gone += [k for k in batch if found.get(k, True)]
    finally:
        fetcher.close()
    with conn.transaction():
        n = conn.execute(
            """UPDATE posts SET body = NULL, author = NULL,
                   extra = (extra - 'quotes' - 'flair') || '{"deleted": true}'
               WHERE source_id = %s AND source_key = ANY(%s)""", (src, gone)).rowcount
        conn.execute(
            """UPDATE threads t SET author = NULL, extra = t.extra || '{"deleted": true}'
               WHERE t.source_id = %s AND 't3_' || t.source_key = ANY(%s)""", (src, gone))
        conn.execute("DELETE FROM raw_pages WHERE source_id = %s AND kind = 'reddit_info'", (src,))
    log.info("Reddit deletion check: %s of %s items now deleted or removed", f"{n:,}", f"{len(keys):,}")


def purge_raw(conn, days: int = RAW_KEEP_DAYS) -> None:
    """Delete parsed Reddit raw pages older than `days` (the posts and threads rows are kept)."""
    n = conn.execute(
        """DELETE FROM raw_pages WHERE source_id = %s AND parsed_at IS NOT NULL
           AND fetched_at < now() - make_interval(days => %s)""", (source_id(conn, SOURCE), days)).rowcount
    log.info("Reddit raw pages older than %d days deleted: %s", days, f"{n:,}")
