"""Blizzard WoW forums (us.forums.blizzard.com), a Discourse site with a public JSON API.

fetch(): categories.json -> category topic lists -> each changed topic's JSON -> its posts.
parse(): raw 'topic' / 'posts' pages -> threads and posts rows.
"""
import logging
import re

import ftfy
from bs4 import BeautifulSoup
from psycopg.types.json import Jsonb

from epic_parse import wow_patches
from epic_parse.db import source_id
from epic_parse.fetch import Fetcher

log = logging.getLogger(__name__)

SOURCE = "blizzard_forums"
BASE = "https://us.forums.blizzard.com/en/wow"

# Top-level categories crawled when none are given (their subcategories are included).
DEFAULT_CATEGORIES = ["gameplay", "classes", "pvp", "lore", "in-development", "community", "wow-classic"]

# Skipped wherever they appear, as a category or a subcategory (along with their
# subcategories). All realm forums, retail and Classic, are skipped too; see _is_realm().
DENY_CATEGORY_NAMES = {
    "Off-Topic",
    "Support",
    "Recruitment",
    "UI and Macro",
    "WoW Classic New Guild Listings",
    "Classic Connections 2004-2010 - Find People Here",
}

POSTS_PER_REQUEST = 20

# Leftovers inside quotes of very old posts, e.g. "10/28/2018 09:08 PMPosted by Snowfox"
OLD_POST_HEADER = re.compile(r"\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2} [AP]M\s*Posted by \w+\s*")


# --------------------------------------------------------------------------- fetch


def _is_realm(category: dict) -> bool:
    """Realm forums (retail and Classic) carry an `is_realm` flag in their metadata."""
    return "is_realm" in (category.get("category_metadata") or {})


def _fetch_categories(fetcher: Fetcher) -> dict[int, dict]:
    _, data = fetcher.get_json(f"{BASE}/categories.json", "categories", params={"include_subcategories": "true"})
    return categories_from_json(data)


def categories_from_json(data: dict | None) -> dict[int, dict]:
    """Return {category_id: {"name", "slug", "parent", "denied"}} for categories and subcategories."""
    cats = {}
    for c in (data or {}).get("category_list", {}).get("categories", []):
        parent_denied = _is_realm(c) or c["name"] in DENY_CATEGORY_NAMES
        cats[c["id"]] = {"name": c["name"], "slug": c["slug"], "parent": None, "denied": parent_denied}
        for sub in c.get("subcategory_list", []):
            denied = parent_denied or _is_realm(sub) or sub["name"] in DENY_CATEGORY_NAMES
            cats[sub["id"]] = {"name": sub["name"], "slug": sub["slug"], "parent": c["name"], "denied": denied}
    return cats


def _is_denied(cats: dict[int, dict], category_id: int) -> bool:
    return cats.get(category_id, {}).get("denied", False)


def _already_have(conn, topic: dict) -> bool:
    """True if this exact version of the topic (same last post time) was fetched before."""
    return conn.execute(
        "SELECT 1 FROM raw_pages WHERE kind = 'topic' AND body->>'id' = %s AND body->>'last_posted_at' = %s LIMIT 1",
        (str(topic["id"]), topic.get("last_posted_at")),
    ).fetchone() is not None


def _fetch_topic(conn, fetcher: Fetcher, src: int, topic_id: int) -> int:
    """Fetch one topic plus any of its posts not already in the database. Returns requests made."""
    _, data = fetcher.get_json(f"{BASE}/t/{topic_id}.json", "topic")
    if not data:
        return 1
    stream = data["post_stream"]["stream"]
    have = {p["id"] for p in data["post_stream"]["posts"]}
    have |= {
        int(r[0])
        for r in conn.execute(
            "SELECT p.source_key FROM posts p JOIN threads t ON t.id = p.thread_id "
            "WHERE t.source_id = %s AND t.source_key = %s",
            (src, str(topic_id)),
        )
    }
    missing = [pid for pid in stream if pid not in have]
    for i in range(0, len(missing), POSTS_PER_REQUEST):
        chunk = missing[i : i + POSTS_PER_REQUEST]
        fetcher.get_json(f"{BASE}/t/{topic_id}/posts.json", "posts", params=[("post_ids[]", pid) for pid in chunk])
    return 1 + -(-len(missing) // POSTS_PER_REQUEST)


def fetch(conn, categories: list[str] | None = None, max_pages: int | None = None,
          max_topics: int | None = None, delay: float = 1.5, **_) -> None:
    """Crawl the given top-level category slugs (default: DEFAULT_CATEGORIES).

    max_pages:  topic-list pages per category (30 topics each), newest activity first
    max_topics: stop after fetching this many new/changed topics in total
    """
    src = source_id(conn, SOURCE)
    fetcher = Fetcher(conn, src, delay=delay)
    try:
        cats = _fetch_categories(fetcher)
        by_slug = {c["slug"]: cid for cid, c in cats.items()}
        fetched = 0
        for slug in categories or DEFAULT_CATEGORIES:
            if slug not in by_slug:
                log.error("Unknown category %r. Known: %s", slug, ", ".join(sorted(by_slug)))
                continue
            if cats[by_slug[slug]]["denied"]:
                log.error("Category %r is on the deny list (realm forum or excluded category), skipping", slug)
                continue
            page = 0
            while max_pages is None or page < max_pages:
                _, data = fetcher.get_json(f"{BASE}/c/{slug}/{by_slug[slug]}.json", "topic_list", params={"page": page})
                topics = (data or {}).get("topic_list", {}).get("topics", [])
                if not topics:
                    break
                for t in topics:
                    if _is_denied(cats, t.get("category_id")) or _already_have(conn, t):
                        continue
                    if max_topics is not None and fetched >= max_topics:
                        log.info("Reached max_topics=%d", max_topics)
                        return
                    requests = _fetch_topic(conn, fetcher, src, t["id"])
                    fetched += 1
                    log.info("[%s p%d] topic %d %r (%d requests)", slug, page, t["id"], t.get("title", "")[:60], requests)
                if not data["topic_list"].get("more_topics_url"):
                    break
                page += 1
        log.info("Done: %d topics fetched", fetched)
    finally:
        fetcher.close()


# --------------------------------------------------------------------------- parse


def clean_text(text: str) -> str:
    """Fix mis-decoded characters (e.g. 'canâ€™t' -> 'can't') and collapse whitespace."""
    return re.sub(r"\s+", " ", ftfy.fix_text(text or "")).strip()


def split_quotes(html: str) -> tuple[str, list[dict]]:
    """Separate a post's own text from the posts it quotes.

    Returns (body_text, [{"user", "post_number", "text"}, ...]).
    """
    soup = BeautifulSoup(html or "", "html.parser")
    quotes = []
    for aside in soup.find_all("aside", class_="quote"):
        block = aside.find("blockquote")
        if block:
            quotes.append({
                "user": aside.get("data-username"),
                "post_number": aside.get("data-post"),
                "text": clean_text(OLD_POST_HEADER.sub("", block.get_text(" ", strip=True))),
            })
        aside.decompose()
    for block in soup.find_all("blockquote"):
        quotes.append({"user": None, "post_number": None,
                       "text": clean_text(OLD_POST_HEADER.sub("", block.get_text(" ", strip=True)))})
        block.decompose()
    return clean_text(soup.get_text(" ", strip=True)), quotes


def latest_categories(conn, src: int) -> dict[int, dict]:
    """Categories from the most recently fetched categories.json."""
    row = conn.execute(
        "SELECT body FROM raw_pages WHERE source_id = %s AND kind = 'categories' AND status = 200 "
        "ORDER BY fetched_at DESC LIMIT 1",
        (src,),
    ).fetchone()
    return categories_from_json(row[0] if row else None)


def realm_from_username(username: str | None) -> str | None:
    """'Rozzezz-malganis' -> 'malganis'. Newer usernames end in an account number instead."""
    if username and "-" in username:
        tail = username.split("-", 1)[1]
        if not tail.isdigit():
            return tail
    return None


def _compact(d: dict) -> dict:
    return {k: v for k, v in d.items() if v not in (None, "", [], {})}


def _upsert_thread(conn, src: int, topic: dict, cats: dict) -> int:
    cat = cats.get(topic.get("category_id"), {})
    author = (topic.get("details") or {}).get("created_by", {}).get("username")
    extra = _compact({
        "category_id": topic.get("category_id"),
        "parent_category": cat.get("parent"),
        "views": topic.get("views"),
        "like_count": topic.get("like_count"),
        "reply_count": topic.get("reply_count"),
        "participant_count": topic.get("participant_count"),
        "tags": topic.get("tags"),
        "closed": topic.get("closed") or None,
        "archived": topic.get("archived") or None,
        "pinned": topic.get("pinned") or None,
    })
    return conn.execute(
        """
        INSERT INTO threads (source_id, source_key, category, title, author, url,
                             created_at, last_posted_at, post_count, extra)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_id, source_key) DO UPDATE SET
            category = EXCLUDED.category, title = EXCLUDED.title,
            author = COALESCE(EXCLUDED.author, threads.author), url = EXCLUDED.url,
            last_posted_at = EXCLUDED.last_posted_at, post_count = EXCLUDED.post_count,
            extra = EXCLUDED.extra, updated_at = now()
        RETURNING id
        """,
        (src, str(topic["id"]), cat.get("name"), clean_text(topic.get("title")), author,
         f"{BASE}/t/{topic.get('slug', 'topic')}/{topic['id']}", topic.get("created_at"),
         topic.get("last_posted_at"), topic.get("posts_count"), Jsonb(extra)),
    ).fetchone()[0]


def _upsert_post(conn, src: int, thread_id: int, raw_id: int, p: dict) -> None:
    body, quotes = split_quotes(p.get("cooked"))
    fields = p.get("user_custom_fields") or {}
    alias = (p.get("poster_alias") or {}).get("alias") or {}
    alias_fields = {f["name"]: f["value"] for f in alias.get("alias_custom_fields", []) if "name" in f}
    likes = next((a.get("count") for a in p.get("actions_summary", []) if a.get("id") == 2), 0)
    extra = _compact({
        "reply_to_post_number": p.get("reply_to_post_number"),
        "likes": likes,
        "reply_count": p.get("reply_count"),
        "reads": p.get("reads"),
        "quotes": quotes,
        "character": alias.get("name") or p.get("alias_username"),
        "realm": alias_fields.get("realm") or realm_from_username(p.get("username")),
        "class": fields.get("class") or alias_fields.get("player_class"),
        "race": fields.get("race") or alias_fields.get("race"),
        "level": fields.get("level") or alias_fields.get("level"),
        "guild": fields.get("guild"),
        "user_title": p.get("user_title"),
        "staff": p.get("staff") or None,
        "moderator": p.get("moderator") or None,
        "trust_level": p.get("trust_level"),
        "edits": (p.get("version") or 1) - 1 or None,
        "hidden": p.get("hidden") or None,
        "deleted": bool(p.get("deleted_at") or p.get("user_deleted")) or None,
    })
    conn.execute(
        """
        INSERT INTO posts (source_id, source_key, thread_id, position, author, body,
                           created_at, updated_at, raw_page_id, extra)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_id, source_key) DO UPDATE SET
            thread_id = EXCLUDED.thread_id, position = EXCLUDED.position, author = EXCLUDED.author,
            body = EXCLUDED.body, updated_at = EXCLUDED.updated_at,
            raw_page_id = EXCLUDED.raw_page_id, extra = EXCLUDED.extra
        """,
        (src, str(p["id"]), thread_id, p.get("post_number"), p.get("username"), body,
         p.get("created_at"), p.get("updated_at"), raw_id, Jsonb(extra)),
    )


def parse(conn, batch_size: int = 200) -> None:
    """Turn every unparsed raw topic/posts page into threads and posts rows."""
    src = source_id(conn, SOURCE)
    cats = latest_categories(conn, src)
    pages = threads = posts = 0
    while True:
        with conn.transaction():
            batch = conn.execute(
                "SELECT id, kind, body FROM raw_pages WHERE source_id = %s AND kind IN ('topic', 'posts') "
                "AND status = 200 AND parsed_at IS NULL ORDER BY id LIMIT %s",
                (src, batch_size),
            ).fetchall()
            if not batch:
                break
            for raw_id, kind, body in batch:
                if kind == "topic":
                    thread_id = _upsert_thread(conn, src, body, cats)
                    threads += 1
                else:
                    first = body["post_stream"]["posts"][0] if body["post_stream"]["posts"] else None
                    row = first and conn.execute(
                        "SELECT id FROM threads WHERE source_id = %s AND source_key = %s",
                        (src, str(first["topic_id"])),
                    ).fetchone()
                    if not row:
                        log.warning("raw page %d: posts for an unknown thread, skipping", raw_id)
                        conn.execute("UPDATE raw_pages SET parsed_at = now() WHERE id = %s", (raw_id,))
                        continue
                    thread_id = row[0]
                for p in body["post_stream"]["posts"]:
                    _upsert_post(conn, src, thread_id, raw_id, p)
                    posts += 1
                conn.execute("UPDATE raw_pages SET parsed_at = now() WHERE id = %s", (raw_id,))
            pages += len(batch)

    # Link replies to the post they answer (needs both posts present, so done last).
    with conn.transaction():
        linked = conn.execute(
            """
            UPDATE posts c SET parent_id = p.id
            FROM posts p
            WHERE c.source_id = %s AND c.parent_id IS NULL
              AND c.extra ? 'reply_to_post_number'
              AND p.thread_id = c.thread_id
              AND p.position = (c.extra->>'reply_to_post_number')::int
            """,
            (src,),
        ).rowcount
    log.info("Parsed %d raw pages: %d thread updates, %d post upserts, %d replies linked",
             pages, threads, posts, linked)
    wow_patches.tag_posts(conn, SOURCE)
