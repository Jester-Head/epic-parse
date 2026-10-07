"""Blizzard's World of Warcraft Profile API (develop.battle.net) for forum posters' characters.

Adds what Raider.IO doesn't have: PvP ratings, achievements with the date each was earned,
collections, lifetime statistics, raid kills with dates, and last login.

Credentials: BLIZZARD_CLIENT_ID and BLIZZARD_CLIENT_SECRET in .env (client-credentials flow;
a token lasts 24 hours and is refreshed automatically). Limit: 36,000 requests per hour.

Per character: summary (stops here on 404, which Blizzard returns for characters that haven't
logged in for a long time, are very low level, or were renamed/transferred), PvP summary plus
each rated bracket, achievements, achievement statistics, mounts, pets, toys, raid encounters.
Responses go to raw_pages whole, except two that are reduced first because of their size:
achievements (~2 MB each) keep the totals plus [achievement id, completed timestamp] pairs,
and mounts/pets/toys keep only their counts.

fetch(): characters that posted in retail forums (same candidates as Raider.IO), newest posters
         first, skipping those fetched within `refresh_days`. Applied as it goes.
parse(): re-applies stored responses to bnet_characters.
"""
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote, urlsplit

import httpx

from epic_parse import milestones
from epic_parse.db import connect, source_id
from epic_parse.fetch import Fetcher
from epic_parse.sources.raiderio import NOT_UNKNOWN_REALM, SINCE, load_candidates

log = logging.getLogger(__name__)

SOURCE = "blizzard_api"
REGION = "us"
API = f"https://{REGION}.api.blizzard.com"
PROFILE = {"namespace": f"profile-{REGION}", "locale": "en_US"}
TOKEN_URL = "https://oauth.battle.net/token"
TOKEN_MAX_AGE = 12 * 3600  # refresh well before the 24 h expiry


class Token:
    """Client-credentials access token shared by all workers."""

    def __init__(self):
        self._lock = threading.Lock()
        self._value: str | None = None
        self._at = 0.0

    def get(self, force: bool = False) -> str:
        with self._lock:
            if force or not self._value or time.monotonic() - self._at > TOKEN_MAX_AGE:
                resp = httpx.post(TOKEN_URL, data={"grant_type": "client_credentials"},
                                  auth=(os.environ["BLIZZARD_CLIENT_ID"], os.environ["BLIZZARD_CLIENT_SECRET"]),
                                  timeout=30)
                resp.raise_for_status()
                self._value, self._at = resp.json()["access_token"], time.monotonic()
            assert self._value is not None
            return self._value


def _compact_achievements(data: dict) -> dict:
    return {
        "total_points": data.get("total_points"),
        "total_quantity": data.get("total_quantity"),
        "completed": [[a["id"], a["completed_timestamp"]] for a in data.get("achievements", [])
                      if a.get("completed_timestamp")],
    }


def _count(key: str):
    return lambda data: {"count": len(data.get(key) or [])}


def _get(fetcher: Fetcher, token: Token, path: str, kind: str, transform=None) -> tuple[int, dict | None, int | None]:
    """GET a Profile API path (or a full href), refreshing the token once on 401."""
    url = path if path.startswith("http") else API + path
    url = url.split("?", 1)[0]  # hrefs carry ?namespace=..., which PROFILE supplies
    for attempt in (1, 2):
        fetcher.client.headers["Authorization"] = f"Bearer {token.get(force=attempt == 2)}"
        raw_id, data = fetcher.get_json(url, kind, params=PROFILE, transform=transform)
        if fetcher.last_status != 401:
            break
    return raw_id, data, fetcher.last_status


def _lookup(conn, fetcher: Fetcher, token: Token, char_id: int, realm: str, name: str,
            milestone_ids: set[int] | None = None) -> bool:
    base = f"/profile/wow/character/{quote(realm)}/{quote(name.lower())}"
    raw_id, summary, status = _get(fetcher, token, base, "bnet_summary")
    conn.execute("UPDATE characters SET bnet_failures = 0 WHERE id = %s", (char_id,))  # got a real answer
    if status != 200 or not summary:
        conn.execute(
            """INSERT INTO bnet_characters (character_id, found, fetched_at, raw_page_id) VALUES (%s, false, now(), %s)
               ON CONFLICT (character_id) DO UPDATE SET found = false, fetched_at = now(), raw_page_id = EXCLUDED.raw_page_id""",
            (char_id, raw_id),
        )
        return False

    pvp = {}
    _, pvp_summary, _ = _get(fetcher, token, base + "/pvp-summary", "bnet_pvp_summary")
    for bracket in (pvp_summary or {}).get("brackets", []):
        _, b, _ = _get(fetcher, token, bracket["href"], "bnet_pvp_bracket")
        if b:
            stats = b.get("season_match_statistics") or {}
            pvp[(b.get("bracket") or {}).get("type") or urlsplit(bracket["href"]).path.rsplit("/", 1)[-1]] = {
                "rating": b.get("rating"), "played": stats.get("played"), "won": stats.get("won"),
                "season": (b.get("season") or {}).get("id"),
            }
    _, ach, _ = _get(fetcher, token, base + "/achievements", "bnet_achievements", transform=_compact_achievements)
    if ach and milestone_ids:
        milestones.milestones_from_achievements(conn, char_id, ach.get("completed", []), milestone_ids)
    counts = {}
    for what in ("mounts", "pets", "toys"):
        _, c, _ = _get(fetcher, token, f"{base}/collections/{what}", f"bnet_{what}", transform=_count(what))
        counts[what] = (c or {}).get("count")
    _get(fetcher, token, base + "/achievements/statistics", "bnet_statistics")
    _get(fetcher, token, base + "/encounters/raids", "bnet_raids")

    conn.execute(
        """INSERT INTO bnet_characters (character_id, found, level, last_login, item_level, achievement_points,
                                        guild, active_spec, honor_level, honorable_kills, pvp, mounts, pets, toys,
                                        achievements_completed, fetched_at, raw_page_id)
           VALUES (%(id)s, true, %(level)s, to_timestamp(%(login)s / 1000.0), %(ilvl)s, %(ap)s, %(guild)s, %(spec)s,
                   %(honor)s, %(hks)s, %(pvp)s::jsonb, %(mounts)s, %(pets)s, %(toys)s, %(ach)s, now(), %(raw)s)
           ON CONFLICT (character_id) DO UPDATE SET found = true, level = EXCLUDED.level,
               last_login = EXCLUDED.last_login, item_level = EXCLUDED.item_level,
               achievement_points = EXCLUDED.achievement_points, guild = EXCLUDED.guild,
               active_spec = EXCLUDED.active_spec, honor_level = EXCLUDED.honor_level,
               honorable_kills = EXCLUDED.honorable_kills, pvp = EXCLUDED.pvp, mounts = EXCLUDED.mounts,
               pets = EXCLUDED.pets, toys = EXCLUDED.toys, achievements_completed = EXCLUDED.achievements_completed,
               fetched_at = now(), raw_page_id = EXCLUDED.raw_page_id""",
        {"id": char_id, "level": summary.get("level"), "login": summary.get("last_login_timestamp"),
         "ilvl": summary.get("equipped_item_level"), "ap": summary.get("achievement_points"),
         "guild": (summary.get("guild") or {}).get("name"), "spec": (summary.get("active_spec") or {}).get("name"),
         "honor": (pvp_summary or {}).get("honor_level"), "hks": (pvp_summary or {}).get("honorable_kills"),
         "pvp": json.dumps(pvp), "ach": len((ach or {}).get("completed", [])) if ach else None,
         "raw": raw_id, **counts},
    )
    conn.execute(
        """UPDATE characters SET class = coalesce(class, %s), race = coalesce(race, %s), faction = coalesce(faction, %s),
               level = coalesce(%s, level) WHERE id = %s""",
        ((summary.get("character_class") or {}).get("name"), (summary.get("race") or {}).get("name"),
         (summary.get("faction") or {}).get("name"), summary.get("level"), char_id),
    )
    return True


def fetch(conn, limit: int | None = None, refresh_days: int = 30, delay: float = 0.5, workers: int = 4,
          retry_failed: bool = False, **_) -> None:
    """Look up forum posters' characters on the Profile API, newest posters first.

    About 8 requests per character; 4 workers x 0.5 s keeps the rate near 8/s, under the
    36,000/hour limit (10/s on average). Characters whose lookup failed after all retries are
    skipped unless `retry_failed`.
    """
    src = source_id(conn, SOURCE)
    load_candidates(conn)
    todo = conn.execute(
        f"""SELECT ch.id, ch.realm, ch.name
            FROM characters ch JOIN candidates c ON c.realm = ch.realm AND lower(c.name) = lower(ch.name)
            LEFT JOIN bnet_characters b ON b.character_id = ch.id
            WHERE ch.region = %(region)s AND {NOT_UNKNOWN_REALM}
              AND (%(retry)s OR ch.bnet_failures = 0)
              AND (b.fetched_at IS NULL OR b.fetched_at < now() - make_interval(days => %(days)s))
            ORDER BY c.last_post DESC
            LIMIT %(limit)s""",
        {"region": REGION, "since": SINCE, "days": refresh_days, "limit": limit, "retry": retry_failed},
    ).fetchall()
    log.info("%d characters to look up on the Blizzard API with %d worker(s)", len(todo), workers)
    token = Token()
    if not milestones.milestone_ids(conn):
        milestones.refresh_achievement_map(conn, token)
    milestone_ids = milestones.milestone_ids(conn)
    progress = {"done": 0, "found": 0, "errors": 0}
    lock = threading.Lock()

    def work(chunk):
        with connect() as wconn:
            wfetcher = Fetcher(wconn, src, delay=delay)
            try:
                for char_id, realm, name in chunk:
                    failed = False
                    try:
                        ok = _lookup(wconn, wfetcher, token, char_id, realm, name, milestone_ids)
                    except Exception as e:  # one bad character shouldn't stop the run
                        log.warning("Blizzard lookup failed for %s-%s, skipped from now on (--retry-failed): %s",
                                    name, realm, e)
                        wconn.execute("UPDATE characters SET bnet_failures = bnet_failures + 1 WHERE id = %s", (char_id,))
                        ok, failed = False, True
                    with lock:
                        progress["done"] += 1
                        progress["found"] += ok
                        progress["errors"] += failed
                        if progress["done"] % 100 == 0:
                            log.info("  %d/%d looked up, %d found, %d errors",
                                     progress["done"], len(todo), progress["found"], progress["errors"])
            finally:
                wfetcher.close()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(work, [todo[i::workers] for i in range(workers)]))
    log.info("Done: %d/%d characters found on the Blizzard API (%d errors)",
             progress["found"], len(todo), progress["errors"])


def parse(conn) -> None:
    log.info("bnet_characters is filled as fetch runs; re-parse from raw_pages isn't implemented yet")
