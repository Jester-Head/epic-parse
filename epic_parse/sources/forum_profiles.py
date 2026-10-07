"""Public profiles of Blizzard forum accounts (us.forums.blizzard.com/en/wow/u/<username>.json).

Each profile lists the account's characters ("aliases": every character on the poster's
Battle.net account that they can post as), with realm, class, race, level, achievement
points and a Classic flag. Blizzard links these, so they are verified alts, unlike names
people type into their "About me". The profile also has account stats (created, last post,
post count, time read) and the "About me" text when filled in.

fetch(): one profile per forum account that posted in a crawled thread, most recent posters
         first; accounts fetched within `refresh_days` are skipped. Applied as it goes.
parse(): re-applies stored profiles (e.g. after changing _apply).
"""
import logging
import re
from urllib.parse import quote

from bs4 import BeautifulSoup

from epic_parse.db import source_id
from epic_parse.fetch import Fetcher
from epic_parse.sources.blizzard_forums import BASE, SOURCE

log = logging.getLogger(__name__)

REGION = "us"

# Forum accounts behind crawled posts (v1-imported authors are character names, not accounts).
ACCOUNTS_SQL = """
    SELECT author, max(created_at) AS last_post
    FROM posts
    WHERE source_id = %(src)s AND NOT extra ? 'v1_import' AND coalesce(author, '') <> ''
    GROUP BY author
"""


def _bio_text(html: str | None) -> str | None:
    text = BeautifulSoup(html or "", "html.parser").get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text) or None


def _apply(conn, username: str, raw_id: int, status: int, body: dict | None) -> int:
    """Store one profile: account row, its characters, and the account -> character links.

    Returns the number of characters listed (0 if the profile is gone).
    """
    if status != 200 or not body or "user" not in body:
        conn.execute(
            """INSERT INTO forum_accounts (username, found, fetched_at, raw_page_id) VALUES (%s, false, now(), %s)
               ON CONFLICT (username) DO UPDATE SET found = false, fetched_at = now(), raw_page_id = EXCLUDED.raw_page_id""",
            (username, raw_id),
        )
        return 0
    u = body["user"]
    aliases = []
    for a in u.get("aliases") or []:
        f = {g["name"]: g["value"] for g in a.get("alias_custom_fields", []) if "name" in g}
        name = (a.get("name") or "").split("-", 1)[0]
        if name and f.get("realm"):
            aliases.append((name, f["realm"], f.get("player_class"), f.get("race"), f.get("level"),
                            f.get("achievement_points"), f.get("classic") == "t"))
    with conn.transaction():
        conn.execute(
            """INSERT INTO forum_accounts (username, found, user_id, display_name, bio, account_created,
                                           last_posted_at, last_seen_at, post_count, time_read_s,
                                           profile_views, alias_count, fetched_at, raw_page_id)
               VALUES (%s, true, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
               ON CONFLICT (username) DO UPDATE SET found = true, user_id = EXCLUDED.user_id,
                   display_name = EXCLUDED.display_name, bio = EXCLUDED.bio,
                   account_created = EXCLUDED.account_created, last_posted_at = EXCLUDED.last_posted_at,
                   last_seen_at = EXCLUDED.last_seen_at, post_count = EXCLUDED.post_count,
                   time_read_s = EXCLUDED.time_read_s, profile_views = EXCLUDED.profile_views,
                   alias_count = EXCLUDED.alias_count, fetched_at = now(), raw_page_id = EXCLUDED.raw_page_id""",
            (username, u.get("id"), u.get("name"), _bio_text(u.get("bio_cooked")), u.get("created_at"),
             u.get("last_posted_at"), u.get("last_seen_at"), u.get("post_count"), u.get("time_read"),
             u.get("profile_view_count"), len(aliases), raw_id),
        )
        if not aliases:
            return 0
        player_id = conn.execute(
            "INSERT INTO players (key) VALUES (%s) ON CONFLICT (key) DO UPDATE SET key = EXCLUDED.key RETURNING id",
            (f"forum:{username}",),
        ).fetchone()[0]
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO characters (region, realm, name, class, race, classic, level, achievement_points)
                   VALUES (%(region)s, realm_slug(%(realm)s), %(name)s, %(class)s, %(race)s, %(classic)s,
                           %(level)s::numeric::int, %(ap)s::numeric::int)
                   ON CONFLICT (region, realm, lower(name)) DO UPDATE SET
                       classic = EXCLUDED.classic, level = EXCLUDED.level,
                       achievement_points = EXCLUDED.achievement_points,
                       class = coalesce(characters.class, %(class)s), race = coalesce(characters.race, %(race)s)""",
                [{"region": REGION, "realm": realm, "name": name, "classic": classic, "level": level or None,
                  "ap": ap or None, "class": cls, "race": race}
                 for name, realm, cls, race, level, ap, classic in aliases],
            )
            cur.executemany(
                """INSERT INTO player_characters (player_id, character_id, how)
                   SELECT %(player)s, id, 'account_alias' FROM characters
                   WHERE region = %(region)s AND realm = realm_slug(%(realm)s) AND lower(name) = lower(%(name)s)
                   ON CONFLICT DO NOTHING""",
                [{"player": player_id, "region": REGION, "realm": realm, "name": name}
                 for name, realm, *_ in aliases],
            )
    return len(aliases)


def fetch(conn, limit: int | None = None, delay: float = 1.5, refresh_days: int = 30, **_) -> None:
    src = source_id(conn, SOURCE)
    todo = conn.execute(
        f"""SELECT a.author FROM ({ACCOUNTS_SQL}) a
            LEFT JOIN forum_accounts fa ON fa.username = a.author
            WHERE fa.fetched_at IS NULL OR fa.fetched_at < now() - make_interval(days => %(days)s)
            ORDER BY a.last_post DESC
            LIMIT %(limit)s""",
        {"src": src, "days": refresh_days, "limit": limit},
    ).fetchall()
    log.info("%d forum profiles to fetch", len(todo))
    fetcher = Fetcher(conn, src, delay=delay)
    try:
        chars = found = 0
        for i, (username,) in enumerate(todo, 1):
            raw_id, body = fetcher.get_json(f"{BASE}/u/{quote(username, safe='')}.json", "user_profile")
            status = conn.execute("SELECT status FROM raw_pages WHERE id = %s", (raw_id,)).fetchone()[0]
            n = _apply(conn, username, raw_id, status, body)
            conn.execute("UPDATE raw_pages SET parsed_at = now() WHERE id = %s", (raw_id,))
            chars += n
            found += status == 200
            if i % 100 == 0:
                log.info("  %d/%d profiles, %d found, %d characters listed", i, len(todo), found, chars)
        log.info("Done: %d/%d profiles found, %d characters listed", found, len(todo), chars)
    finally:
        fetcher.close()


def parse(conn) -> None:
    """Re-apply the latest stored profile of every account."""
    src = source_id(conn, SOURCE)
    rows = conn.execute(
        """SELECT DISTINCT ON (fa.username) fa.username, r.id, r.status, r.body
           FROM forum_accounts fa JOIN raw_pages r ON r.id = fa.raw_page_id
           WHERE r.source_id = %s ORDER BY fa.username""",
        (src,),
    ).fetchall()
    chars = sum(_apply(conn, username, rid, status, body) for username, rid, status, body in rows)
    log.info("Re-applied %d profiles (%d characters listed)", len(rows), chars)
