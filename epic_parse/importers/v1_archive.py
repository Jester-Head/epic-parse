"""Import data files from the v1 project (git tag v1-archive).

Imported rows are marked with extra->'v1_import' and never overwrite crawled rows.
"""
import ast
import csv
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from epic_parse.db import bulk_mode, source_id
from epic_parse.sources import blizzard_forums

log = logging.getLogger(__name__)

KINDS = {
    "forum-log": ("blizzard_forums", "v1_forum_item"),
    "youtube-json": ("youtube", "v1_yt_thread"),
    "youtube-csv": ("youtube", "v1_yt_csv_row"),
}

LOG_LINE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ ")
MAX_ITEM_LINES = 5000
NUL_ESCAPE = re.compile(r"(?<!\\)((?:\\\\)*)\\u0000")

CSV_COLUMNS = {
    "commentId": "comment_id", "channelId": "channel_id", "channelName": "channel_name",
    "videoId": "video_id", "videoTitle": "video_title", "textOriginal": "text",
    "likeCount": "like_count", "publishedAt": "published_at", "updatedAt": "updated_at",
}


def _forum_log_items(path: Path):
    """Python-dict items that Scrapy printed at DEBUG level, one per scraped post."""
    timestamp, buf = None, None
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = LOG_LINE.match(line)
            if m:
                timestamp, buf = m.group(1), None
                continue
            if buf is None:
                if not line.startswith("{'"):
                    continue
                buf = []
            buf.append(line)
            if not line.rstrip().endswith("}"):
                continue
            try:
                item = ast.literal_eval("".join(buf))
            except (SyntaxError, ValueError):
                if len(buf) > MAX_ITEM_LINES:
                    buf = None
                continue  # a '}' inside a string; keep collecting
            buf = None
            if isinstance(item, dict) and "post_id" in item:
                yield item.get("url") or "", item, timestamp


def _json_array(path: Path, chunk: int = 1 << 22):
    """Stream objects out of a huge top-level JSON array without loading it all."""
    decoder = json.JSONDecoder()
    with open(path, encoding="utf-8") as f:
        buf, pos, eof = "", 0, False
        while True:
            if not eof and len(buf) - pos < chunk // 4:
                more = f.read(chunk)
                eof = not more
                buf, pos = buf[pos:] + more, 0
            while pos < len(buf) and buf[pos] in " \t\r\n,[]":
                pos += 1
            if pos >= len(buf):
                if eof:
                    return
                continue
            try:
                obj, pos = decoder.raw_decode(buf, pos)
            except json.JSONDecodeError:
                if eof:
                    raise
                more = f.read(chunk)  # object spans past the buffer
                eof = not more
                buf, pos = buf[pos:] + more, 0
                continue
            yield obj


def _youtube_threads(path: Path):
    for obj in _json_array(path):
        oid = (obj.pop("_id", None) or {}).get("$oid")
        # A MongoDB ObjectId starts with its creation time: the moment v1 stored this record.
        fetched = datetime.fromtimestamp(int(oid[:8], 16), timezone.utc) if oid else None
        video = obj.get("snippet", {}).get("videoId")
        yield f"https://www.youtube.com/watch?v={video}&lc={obj.get('id')}", obj, fetched


def _youtube_csv_rows(path: Path):
    csv.field_size_limit(2**31 - 1)
    fetched = datetime.fromtimestamp(os.path.getmtime(path), timezone.utc)
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        for row in csv.DictReader(f):
            row = {CSV_COLUMNS.get(k, k): v for k, v in row.items() if k}
            yield f"https://www.youtube.com/watch?v={row.get('video_id')}&lc={row.get('comment_id')}", row, fetched


READERS = {"forum-log": _forum_log_items, "youtube-json": _youtube_threads, "youtube-csv": _youtube_csv_rows}


def _json_text(obj) -> str:
    # jsonb can't store NUL. The regex skips a literal backslash-u0000 typed in a comment.
    text = json.dumps(obj, ensure_ascii=False)
    return NUL_ESCAPE.sub(r"\1", text) if "\\u0000" in text else text


def load(conn, what: str, path: str) -> int | None:
    """Stream a file into raw_pages. Returns rows loaded, or None if already imported."""
    source_name, kind = KINDS[what]
    file = Path(path).resolve()
    src = source_id(conn, source_name)
    if conn.execute("SELECT 1 FROM imports WHERE path = %s AND kind = %s", (str(file), kind)).fetchone():
        log.warning("%s was already imported as %s, skipping", file, kind)
        return None
    log.info("Loading %s (%.1f GB) into raw_pages...", file.name, file.stat().st_size / 1e9)
    n = 0
    with conn.transaction():
        import_id = conn.execute(
            "INSERT INTO imports (source_id, path, kind) VALUES (%s, %s, %s) RETURNING id", (src, str(file), kind)
        ).fetchone()[0]
        with conn.cursor().copy(
            "COPY raw_pages (source_id, kind, url, status, body, fetched_at, import_id) FROM STDIN"
        ) as copy:
            for url, body, fetched in READERS[what](file):
                copy.write_row((src, kind, url, 200, _json_text(body), fetched, import_id))
                n += 1
                if n % 100_000 == 0:
                    log.info("  %s rows...", f"{n:,}")
        conn.execute("UPDATE imports SET rows = %s, finished_at = now() WHERE id = %s", (n, import_id))
    log.info("Loaded %s rows from %s", f"{n:,}", file.name)
    return n


def parse_forum(conn) -> None:
    """raw v1_forum_item rows -> threads/posts (realm forums and other denied categories skipped)."""
    src = source_id(conn, "blizzard_forums")
    cats = blizzard_forums.latest_categories(conn, src)
    if not cats:
        raise RuntimeError("No categories.json fetched yet; run a small `fetch blizzard` first")
    denied = sorted({c["name"] for c in cats.values() if c["denied"]})
    with conn.transaction(), bulk_mode(conn):
        conn.execute(
            """
            CREATE TEMP TABLE v1_items ON COMMIT DROP AS
            SELECT DISTINCT ON (body->>'post_id') id AS raw_id, body
            FROM raw_pages
            WHERE source_id = %(src)s AND kind = 'v1_forum_item' AND parsed_at IS NULL
              AND body->>'thread_id' IS NOT NULL
              AND NOT (coalesce(body->>'forum_name', '') = ANY(%(denied)s))
            ORDER BY body->>'post_id', id DESC
            """,
            {"src": src, "denied": denied},
        )
        conn.execute("ANALYZE v1_items")
        threads = conn.execute(
            """
            INSERT INTO threads (source_id, source_key, category, url, created_at, last_posted_at, post_count, extra)
            SELECT %(src)s, body->>'thread_id', max(body->>'forum_name'),
                   'https://us.forums.blizzard.com/en/wow/t/' || (body->>'thread_id'),
                   min(NULLIF(body->>'date_created', '')::timestamptz),
                   max(NULLIF(body->>'date_created', '')::timestamptz),
                   count(*), '{"v1_import": true}'::jsonb
            FROM v1_items GROUP BY body->>'thread_id'
            ON CONFLICT (source_id, source_key) DO UPDATE SET
                category = EXCLUDED.category, created_at = EXCLUDED.created_at,
                last_posted_at = EXCLUDED.last_posted_at, post_count = EXCLUDED.post_count, updated_at = now()
            WHERE threads.extra ? 'v1_import'
            """,
            {"src": src},
        ).rowcount
        posts = conn.execute(
            """
            INSERT INTO posts (source_id, source_key, thread_id, author, body, created_at, updated_at, raw_page_id, extra)
            SELECT %(src)s, i.body->>'post_id', t.id, i.body->>'username', i.body->>'comment_text',
                   NULLIF(i.body->>'date_created', '')::timestamptz, NULLIF(i.body->>'date_updated', '')::timestamptz,
                   i.raw_id,
                   jsonb_strip_nulls(jsonb_build_object(
                       'v1_import', true,
                       'likes', (NULLIF(i.body->>'likes', ''))::numeric::int,
                       'reply_count', (NULLIF(i.body->>'reply_count', ''))::numeric::int,
                       'class', i.body->>'player_class',
                       'race', i.body->>'race',
                       'realm', CASE WHEN substring(i.body->>'username' FROM '^[^-]+-(.+)$') !~ '^[0-9]+$'
                                     THEN substring(i.body->>'username' FROM '^[^-]+-(.+)$') END,
                       'user_title', i.body->>'user_title',
                       'staff', CASE WHEN (i.body->>'staff')::boolean THEN true END,
                       'quotes', CASE WHEN coalesce(i.body->>'quoted_text', '') <> '' THEN (
                           SELECT jsonb_agg(jsonb_build_object('text', q))
                           FROM unnest(string_to_array(i.body->>'quoted_text', '|')) AS q) END,
                       'game_version', i.body->>'game_version',
                       'expansion', i.body->>'expansion_name',
                       'patch', NULLIF(i.body->>'patch_version', 'Unknown')
                   ))
            FROM v1_items i
            JOIN threads t ON t.source_id = %(src)s AND t.source_key = i.body->>'thread_id'
            ON CONFLICT (source_id, source_key) DO UPDATE SET
                thread_id = EXCLUDED.thread_id, author = EXCLUDED.author, body = EXCLUDED.body,
                created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
                raw_page_id = EXCLUDED.raw_page_id, extra = EXCLUDED.extra
            WHERE posts.extra ? 'v1_import'
            """,
            {"src": src},
        ).rowcount
        skipped = conn.execute(
            "SELECT count(*) FROM raw_pages WHERE source_id = %s AND kind = 'v1_forum_item' AND parsed_at IS NULL "
            "AND coalesce(body->>'forum_name', '') = ANY(%s)",
            (src, denied),
        ).fetchone()[0]
        conn.execute(
            "UPDATE raw_pages SET parsed_at = now() WHERE source_id = %s AND kind = 'v1_forum_item' AND parsed_at IS NULL",
            (src,),
        )
    log.info("Forum import: %s threads and %s posts upserted; %s raw items skipped (realm/denied forums)",
             f"{threads:,}", f"{posts:,}", f"{skipped:,}")


def parse_youtube(conn) -> None:
    """All v1 YouTube raw rows -> threads (one per video) and posts (comments and replies).

    Rebuilds from every v1 YouTube raw row each time, because the JSON export (authors,
    replies) and the CSV exports (video titles, channel names) complete each other.
    """
    src = source_id(conn, "youtube")
    log.info("Parsing YouTube raw rows (merging API export and CSVs)...")
    with conn.transaction(), bulk_mode(conn):
        conn.execute(
            """
            CREATE TEMP TABLE yt_all ON COMMIT DROP AS
            SELECT c->>'id' AS comment_id, NULL::text AS parent_comment,
                   s->>'videoId' AS video_id, s->>'channelId' AS channel_id,
                   NULL::text AS channel_name, NULL::text AS video_title,
                   s->>'authorDisplayName' AS author, s->'authorChannelId'->>'value' AS author_channel_id,
                   s->>'textOriginal' AS text, (s->>'likeCount')::int AS likes,
                   (r.body->'snippet'->>'totalReplyCount')::int AS reply_count,
                   (s->>'publishedAt')::timestamptz AS published_at, (s->>'updatedAt')::timestamptz AS updated_at,
                   r.id AS raw_id, r.fetched_at
            FROM raw_pages r, LATERAL (SELECT r.body->'snippet'->'topLevelComment' AS c) tc,
                 LATERAL (SELECT tc.c->'snippet' AS s) sn
            WHERE r.source_id = %(src)s AND r.kind = 'v1_yt_thread'
            UNION ALL
            SELECT rc->>'id', rc->'snippet'->>'parentId',
                   rc->'snippet'->>'videoId', rc->'snippet'->>'channelId', NULL, NULL,
                   rc->'snippet'->>'authorDisplayName', rc->'snippet'->'authorChannelId'->>'value',
                   rc->'snippet'->>'textOriginal', (rc->'snippet'->>'likeCount')::int, NULL,
                   (rc->'snippet'->>'publishedAt')::timestamptz, (rc->'snippet'->>'updatedAt')::timestamptz,
                   r.id, r.fetched_at
            FROM raw_pages r, jsonb_array_elements(coalesce(r.body->'replies'->'comments', '[]')) AS rc
            WHERE r.source_id = %(src)s AND r.kind = 'v1_yt_thread'
            UNION ALL
            SELECT body->>'comment_id', NULL, body->>'video_id', body->>'channel_id',
                   body->>'channel_name', body->>'video_title', NULL, NULL,
                   body->>'text', NULLIF(body->>'like_count', '')::numeric::int, NULL,
                   NULLIF(body->>'published_at', '')::timestamptz, NULLIF(body->>'updated_at', '')::timestamptz,
                   id, fetched_at
            FROM raw_pages
            WHERE source_id = %(src)s AND kind = 'v1_yt_csv_row'
            """,
            {"src": src},
        )
        # One row per comment: text/likes from the newest copy, the rest from whichever copy has it.
        conn.execute(
            """
            CREATE TEMP TABLE yt ON COMMIT DROP AS
            SELECT DISTINCT ON (comment_id)
                   comment_id, video_id, text, likes, published_at, updated_at, raw_id,
                   max(parent_comment) OVER w AS parent_comment, max(author) OVER w AS author,
                   max(author_channel_id) OVER w AS author_channel_id, max(reply_count) OVER w AS reply_count,
                   max(channel_id) OVER w AS channel_id, max(channel_name) OVER w AS channel_name,
                   max(video_title) OVER w AS video_title
            FROM yt_all
            WHERE comment_id IS NOT NULL AND video_id IS NOT NULL
            WINDOW w AS (PARTITION BY comment_id)
            ORDER BY comment_id, fetched_at DESC
            """
        )
        conn.execute("ANALYZE yt")
        log.info("  merged comments ready, writing videos...")
        threads = conn.execute(
            """
            INSERT INTO threads (source_id, source_key, category, title, url, post_count, extra)
            SELECT %(src)s, video_id, max(channel_name), max(video_title),
                   'https://www.youtube.com/watch?v=' || video_id, count(*),
                   jsonb_strip_nulls(jsonb_build_object('v1_import', true, 'channel_id', max(channel_id)))
            FROM yt GROUP BY video_id
            ON CONFLICT (source_id, source_key) DO UPDATE SET
                category = EXCLUDED.category, title = EXCLUDED.title,
                post_count = EXCLUDED.post_count, extra = EXCLUDED.extra, updated_at = now()
            WHERE threads.extra ? 'v1_import'
            """,
            {"src": src},
        ).rowcount
        log.info("  writing comments...")
        posts = conn.execute(
            """
            INSERT INTO posts (source_id, source_key, thread_id, author, body, created_at, updated_at, raw_page_id, extra)
            SELECT %(src)s, y.comment_id, t.id, y.author, y.text, y.published_at, y.updated_at, y.raw_id,
                   jsonb_strip_nulls(jsonb_build_object(
                       'v1_import', true, 'likes', y.likes, 'reply_count', y.reply_count,
                       'author_channel_id', y.author_channel_id, 'parent_comment', y.parent_comment))
            FROM yt y JOIN threads t ON t.source_id = %(src)s AND t.source_key = y.video_id
            ON CONFLICT (source_id, source_key) DO UPDATE SET
                thread_id = EXCLUDED.thread_id, author = EXCLUDED.author, body = EXCLUDED.body,
                created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
                raw_page_id = EXCLUDED.raw_page_id, extra = EXCLUDED.extra
            WHERE posts.extra ? 'v1_import'
            """,
            {"src": src},
        ).rowcount
        log.info("  linking replies...")
        # Join through the temp table's plain columns so both sides use the
        # (source_id, source_key) index; joining on extra->>'parent_comment' made
        # Postgres compare every reply with every post.
        linked = conn.execute(
            """
            UPDATE posts c SET parent_id = p.id
            FROM yt y
            JOIN posts p ON p.source_id = %(src)s AND p.source_key = y.parent_comment
            WHERE y.parent_comment IS NOT NULL
              AND c.source_id = %(src)s AND c.source_key = y.comment_id
              AND c.parent_id IS DISTINCT FROM p.id
            """,
            {"src": src},
        ).rowcount
        conn.execute(
            "UPDATE raw_pages SET parsed_at = now() WHERE source_id = %s AND kind LIKE 'v1_yt_%%' AND parsed_at IS NULL",
            (src,),
        )
    log.info("YouTube import: %s videos and %s comments upserted, %s replies linked",
             f"{threads:,}", f"{posts:,}", f"{linked:,}")


def run(conn, what: str, path: str) -> None:
    load(conn, what, path)
    if what == "forum-log":
        parse_forum(conn)
    else:
        parse_youtube(conn)
