"""Dates people reached Mythic+ milestones, and the dates of their best runs.

Raider.IO only has a character's score as it is now (plus final scores of past seasons), so weekly
history can't be rebuilt for someone tracked late. Two dated sources fill part of that gap:

- Blizzard achievements: the moment an account earned each season's Keystone Explorer, Conqueror,
  Master, Hero, Legend and Myth (`character_milestones`). Achievements are account-wide, so the
  date is when the account first earned it, possibly on another character.
- Raider.IO best runs: each dungeon's best run with its completion date (`character_best_runs`).
  Only runs that are still a character's best are visible, so early weeks are under-represented;
  every best run ever seen is kept, so weekly snapshots build an upgrade history over time.

refresh_achievement_map(): Blizzard achievement id -> (milestone, Raider.IO season), from the
                           Game Data API achievement index.
milestones_from_achievements(): character_milestones from one character's compacted achievements.
store_best_runs(): character_best_runs from one Raider.IO profile response.
backfill(): fill both tables from responses already stored in raw_pages.
"""
import logging
import re
from urllib.parse import unquote, urlsplit

import httpx

from epic_parse.db import source_id

log = logging.getLogger(__name__)

EXPANSION_SLUGS = {"Battle for Azeroth": "bfa", "Shadowlands": "sl", "Dragonflight": "df",
                   "The War Within": "tww", "Midnight": "mn"}
SEASON_NUMBERS = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
SEASONAL = re.compile(r"^(?P<exp>Battle for Azeroth|Shadowlands|Dragonflight|The War Within|Midnight) "
                      r"Keystone (?P<m>Explorer|Conqueror|Master|Hero|Legend|Myth): Season (?P<n>\w+)$")
LEGION = {"Keystone Master", "Keystone Conqueror"}  # expansion-wide, not per season


def _season_slug(expansion: str, number: str) -> str | None:
    n = SEASON_NUMBERS.get(number) or (int(number) if number.isdigit() else None)
    return f"season-{EXPANSION_SLUGS[expansion]}-{n}" if n else None


def refresh_achievement_map(conn, token) -> int:
    """(Re)build milestone_achievements from Blizzard's achievement index."""
    resp = httpx.get("https://us.api.blizzard.com/data/wow/achievement/index",
                     params={"namespace": "static-us", "locale": "en_US"},
                     headers={"Authorization": f"Bearer {token.get()}"}, timeout=60)
    resp.raise_for_status()
    rows = []
    for a in resp.json().get("achievements", []):
        name = a.get("name") or ""
        m = SEASONAL.match(name)
        if m:
            rows.append((a["id"], name, f"Keystone {m['m']}", _season_slug(m["exp"], m["n"])))
        elif name in LEGION:
            rows.append((a["id"], name, name, None))
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO milestone_achievements (achievement_id, name, milestone, season) VALUES (%s, %s, %s, %s)
               ON CONFLICT (achievement_id) DO UPDATE SET name = EXCLUDED.name, milestone = EXCLUDED.milestone,
                   season = EXCLUDED.season""",
            rows,
        )
    log.info("Milestone achievements mapped: %d", len(rows))
    return len(rows)


def milestone_ids(conn) -> set[int]:
    return {r[0] for r in conn.execute("SELECT achievement_id FROM milestone_achievements")}


def milestones_from_achievements(conn, char_id: int, completed: list, ids: set[int], fetched_at=None) -> int:
    """Store the milestone achievements in a compacted [[id, ms timestamp], ...] list.

    `fetched_at` is when Blizzard returned the list (default now); it decides when the row expires.
    """
    rows = [(char_id, a, ts) for a, ts in completed if a in ids]
    if rows:
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO character_milestones (character_id, achievement_id, milestone, season, achieved_at, fetched_at)
                   SELECT %s, achievement_id, milestone, season, to_timestamp(%s / 1000.0), coalesce(%s, now())
                   FROM milestone_achievements WHERE achievement_id = %s
                   ON CONFLICT (character_id, achievement_id) DO UPDATE SET achieved_at = EXCLUDED.achieved_at,
                       fetched_at = EXCLUDED.fetched_at""",
                [(c, ts, fetched_at, a) for c, a, ts in rows],
            )
    return len(rows)


def store_best_runs(conn, char_id: int, season: str | None, body: dict | None) -> int:
    """Keep every best run seen for a character (one per dungeon per lookup; upgrades add rows)."""
    runs = (body or {}).get("mythic_plus_best_runs") or []
    if not season or not runs:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO character_best_runs (character_id, keystone_run_id, season, dungeon, mythic_level, score,
                                                completed_at, clear_time_ms, par_time_ms, upgrades, spec, role)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (character_id, keystone_run_id) DO NOTHING""",
            [(char_id, r.get("keystone_run_id"), season, r.get("short_name"), r.get("mythic_level"), r.get("score"),
              r.get("completed_at"), r.get("clear_time_ms"), r.get("par_time_ms"), r.get("num_keystone_upgrades"),
              (r.get("spec") or {}).get("name"), r.get("role"))
             for r in runs if r.get("keystone_run_id")],
        )
    return len(runs)


def backfill(conn, token) -> None:
    """Fill character_milestones and character_best_runs from responses already in raw_pages."""
    refresh_achievement_map(conn, token)
    ids = milestone_ids(conn)

    # Blizzard achievements: raw pages are keyed by URL (.../character/<realm>/<name>/achievements).
    chars = {(realm, name.lower()): cid for cid, realm, name in
             conn.execute("SELECT id, realm, name FROM characters WHERE region = 'us'")}
    src = source_id(conn, "blizzard_api")
    rows = conn.execute(  # keep only the milestone pairs in the database; full lists are ~1,700 entries each
        """SELECT DISTINCT ON (r.url) r.url, r.fetched_at,
                  (SELECT coalesce(jsonb_agg(e), '[]') FROM jsonb_array_elements(r.body->'completed') e
                   WHERE (e->>0)::int = ANY(%(ids)s))
           FROM raw_pages r WHERE r.source_id = %(src)s AND r.kind = 'bnet_achievements' AND r.status = 200
           ORDER BY r.url, r.id DESC""",
        {"src": src, "ids": list(ids)},
    ).fetchall()
    pages = stored = 0
    with conn.transaction():
        for url, fetched_at, completed in rows:
            parts = urlsplit(url).path.split("/")  # ['', 'profile', 'wow', 'character', realm, name, 'achievements']
            cid = chars.get((unquote(parts[4]), unquote(parts[5]).lower()))
            if cid and completed:
                stored += milestones_from_achievements(conn, cid, completed, ids, fetched_at)
                pages += 1
    log.info("Milestones backfilled: %s from %s characters' achievements", f"{stored:,}", f"{pages:,}")

    # Raider.IO best runs from stored snapshots (each snapshot row knows its character and season).
    n = 0
    with conn.transaction():
        for char_id, season, body in conn.execute(
            """SELECT s.character_id, s.season, r.body FROM character_snapshots s
               JOIN raw_pages r ON r.id = s.raw_page_id WHERE r.status = 200"""
        ).fetchall():
            n += store_best_runs(conn, char_id, season, body)
    log.info("Best runs backfilled: %s runs from stored snapshots", f"{n:,}")
    for table in ("milestone_achievements", "character_milestones", "character_best_runs"):
        conn.execute(f"ANALYZE {table}")  # fresh statistics, or joins on these tables get very slow plans
