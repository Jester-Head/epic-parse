"""Raider.IO (raider.io): Mythic+ scores and raid progress for characters that posted.

API terms (raider.io/api): community and personal use; anything public built on this data
must link to raider.io; no reselling. Requests without a key are rate limited (HTTP 429 with
Retry-After, which the Fetcher honours). A free app key from raider.io/settings/apps raises
the limit: put it in .env as RAIDERIO_KEY.

fetch(): season list and percentile cutoffs, then one profile lookup per character that
         posted in a retail forum (newest posters first). Every response goes to raw_pages.
parse(): re-applies the latest raw profile of each character to the characters table
         (fetch already does this as it goes; parse is for re-processing).
"""
import json
import logging
import os
import re

from epic_parse.db import source_id
from epic_parse.fetch import Fetcher

log = logging.getLogger(__name__)

SOURCE = "raiderio"
BASE = "https://raider.io/api/v1"
REGION = "us"
EXPANSIONS = [7, 8, 9, 10, 11]  # Battle for Azeroth through Midnight
MAIN_SEASON = re.compile(r"^season-(bfa|sl|df|tww|mn)-\d+$")  # skips event variants like '-break-the-meta'
PERCENTILES = {"p999": 99.9, "p990": 99.0, "p900": 90.0, "p750": 75.0, "p600": 60.0}
SINCE = "2018-09-01"  # BfA Season 1 start (Raider.IO has no percentile cutoffs before Shadowlands S3)

# Characters behind retail forum posts. Crawled posts store the posting character as
# 'Name-realm' in extra.character; v1-imported usernames are 'Name-realm' themselves.
CANDIDATES_SQL = """
    SELECT realm_slug(extra->>'realm') AS realm,
           split_part(coalesce(extra->>'character', author), '-', 1) AS name,
           max(created_at) AS last_post
    FROM posts
    WHERE source_id = (SELECT id FROM sources WHERE name = 'blizzard_forums')
      AND extra->>'game_version' = 'retail' AND extra ? 'realm' AND created_at >= %(since)s
    GROUP BY 1, 2
"""


def _params(**params):
    key = os.environ.get("RAIDERIO_KEY")
    return {**params, "access_key": key} if key else params


def _fetch_seasons(conn, fetcher: Fetcher) -> list[str]:
    """Store the main M+ seasons and their percentile cutoffs. Returns season slugs, oldest first."""
    for exp in EXPANSIONS:
        _, data = fetcher.get_json(f"{BASE}/mythic-plus/static-data", "static_data", params=_params(expansion_id=exp))
        for s in (data or {}).get("seasons", []):
            if MAIN_SEASON.match(s["slug"]):
                conn.execute(
                    """INSERT INTO mplus_seasons (slug, name, expansion_id, starts, ends) VALUES (%s, %s, %s, %s, %s)
                       ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name, starts = EXCLUDED.starts, ends = EXCLUDED.ends""",
                    (s["slug"], s.get("name"), exp, (s.get("starts") or {}).get(REGION), (s.get("ends") or {}).get(REGION)),
                )
    seasons = [r[0] for r in conn.execute("SELECT slug FROM mplus_seasons ORDER BY starts")]
    # Cutoffs: refresh seasons that are still running or that we don't have yet.
    for slug in conn.execute(
        "SELECT slug FROM mplus_seasons s WHERE ends > now() "
        "OR NOT EXISTS (SELECT 1 FROM mplus_cutoffs c WHERE c.season = s.slug) ORDER BY starts"
    ).fetchall():
        _, data = fetcher.get_json(f"{BASE}/mythic-plus/season-cutoffs", "cutoffs",
                                   params=_params(region=REGION, season=slug[0]))
        _store_curve(conn, slug[0], (data or {}).get("cutoffs") or {})
        for key, pct in PERCENTILES.items():
            band = ((data or {}).get("cutoffs", {}).get(key) or {}).get("all") or {}
            if band.get("quantileMinValue") is not None:
                conn.execute(
                    """INSERT INTO mplus_cutoffs (season, percentile, min_score, population) VALUES (%s, %s, %s, %s)
                       ON CONFLICT (season, percentile) DO UPDATE SET min_score = EXCLUDED.min_score,
                           population = EXCLUDED.population, fetched_at = now()""",
                    (slug[0], pct, band["quantileMinValue"], band.get("quantilePopulationCount")),
                )
    return seasons


def _store_curve(conn, season: str, cutoffs: dict) -> None:
    """Save every (score, share of players at or above) point and the cutoff history."""
    with conn.transaction():
        for key, value in cutoffs.items():
            if not isinstance(value, dict):
                continue
            band = value.get("all") or (value.get("cutoffs") or {}).get("all") or {}
            if band.get("quantileMinValue") is not None and band.get("quantilePopulationFraction") is not None:
                conn.execute(
                    """INSERT INTO mplus_percentile_points (season, point, min_score, fraction, population)
                       VALUES (%s, %s, %s, %s, %s)
                       ON CONFLICT (season, point) DO UPDATE SET min_score = EXCLUDED.min_score,
                           fraction = EXCLUDED.fraction, population = EXCLUDED.population""",
                    (season, key, band["quantileMinValue"], band["quantilePopulationFraction"],
                     band.get("quantilePopulationCount")),
                )
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO mplus_cutoff_history (season, percentile, at, min_score, players)
                   VALUES (%s, %s, to_timestamp(%s / 1000.0), %s, %s) ON CONFLICT DO NOTHING""",
                [(season, PERCENTILES[key], pt["x"], pt["y"], pt.get("total"))
                 for key, line in (cutoffs.get("graphData") or {}).items() if key in PERCENTILES
                 for pt in line.get("data", [])],
            )


def _apply_profile(conn, char_id: int, raw_id: int, status: int, body: dict | None) -> bool:
    """Write one raw profile response onto its characters row. Returns found."""
    if status != 200 or not body:
        conn.execute("UPDATE characters SET found = false, looked_up_at = now(), raw_page_id = %s WHERE id = %s",
                     (raw_id, char_id))
        return False
    mplus = {s["season"]: (s.get("scores") or {}).get("all", 0) for s in body.get("mythic_plus_scores_by_season", [])}
    raid = {
        slug: {k: p.get(k) for k in ("summary", "normal_bosses_killed", "heroic_bosses_killed",
                                      "mythic_bosses_killed", "total_bosses")}
        for slug, p in (body.get("raid_progression") or {}).items()
    }
    conn.execute(
        """UPDATE characters SET found = true, class = %s, spec = %s, race = %s, faction = %s,
               mplus = %s::jsonb, raid = %s::jsonb, looked_up_at = now(), raw_page_id = %s
           WHERE id = %s""",
        (body.get("class"), body.get("active_spec_name"), body.get("race"), body.get("faction"),
         json.dumps(mplus), json.dumps(raid), raw_id, char_id),
    )
    return True


def fetch(conn, limit: int | None = None, refresh_days: int = 30, delay: float = 1.2, **_) -> None:
    """Look up characters that posted in retail forums since SINCE, newest posters first.

    Characters looked up within `refresh_days` are skipped. `limit` caps lookups per run.
    """
    src = source_id(conn, SOURCE)
    fetcher = Fetcher(conn, src, delay=delay)
    try:
        seasons = _fetch_seasons(conn, fetcher)
        fields = "mythic_plus_scores_by_season:" + ":".join(seasons) + \
                 ",raid_progression:current-expansion:previous-expansion"
        conn.execute(
            f"""INSERT INTO characters (region, realm, name)
                SELECT %(region)s, realm, name FROM ({CANDIDATES_SQL}) c WHERE name <> '' AND realm <> ''
                ON CONFLICT DO NOTHING""",
            {"region": REGION, "since": SINCE},
        )
        todo = conn.execute(
            f"""SELECT ch.id, ch.realm, ch.name
                FROM characters ch JOIN ({CANDIDATES_SQL}) c ON c.realm = ch.realm AND lower(c.name) = lower(ch.name)
                WHERE ch.region = %(region)s
                  AND (ch.looked_up_at IS NULL OR ch.looked_up_at < now() - make_interval(days => %(days)s))
                ORDER BY c.last_post DESC
                LIMIT %(limit)s""",
            {"region": REGION, "since": SINCE, "days": refresh_days, "limit": limit},
        ).fetchall()
        log.info("%d characters to look up (seasons: %s)", len(todo), ", ".join(seasons))
        found = 0
        for i, (char_id, realm, name) in enumerate(todo, 1):
            raw_id, body = fetcher.get_json(f"{BASE}/characters/profile", "character",
                                            params=_params(region=REGION, realm=realm, name=name, fields=fields))
            status = conn.execute("SELECT status FROM raw_pages WHERE id = %s", (raw_id,)).fetchone()[0]
            found += _apply_profile(conn, char_id, raw_id, status, body)
            if i % 100 == 0:
                log.info("  %d/%d looked up, %d found", i, len(todo), found)
        log.info("Done: %d/%d characters found on Raider.IO", found, len(todo))
    finally:
        fetcher.close()


def parse(conn) -> None:
    """Re-apply each character's latest raw profile (e.g. after changing _apply_profile)."""
    rows = conn.execute(
        """SELECT DISTINCT ON (ch.id) ch.id, r.id, r.status, r.body
           FROM characters ch JOIN raw_pages r ON r.id = ch.raw_page_id
           ORDER BY ch.id"""
    ).fetchall()
    with conn.transaction():
        found = sum(_apply_profile(conn, cid, rid, status, body) for cid, rid, status, body in rows)
    log.info("Re-applied %d profiles (%d found)", len(rows), found)


def link_forum_players(conn) -> int:
    """Each forum account becomes a player, linked to every character it has posted as.

    Only crawled posts carry the account name separately from the character; v1-imported
    usernames were the character itself, so those can't be grouped.
    """
    with conn.transaction():
        # One pass over posts to get each distinct (account, character); every join after
        # this is on plain columns. (Joining posts to characters directly on computed
        # expressions made Postgres compare every post with every character.)
        conn.execute(
            """CREATE TEMP TABLE posters ON COMMIT DROP AS
               SELECT author, realm_slug(extra->>'realm') AS realm,
                      split_part(extra->>'character', '-', 1) AS name,
                      lower(split_part(extra->>'character', '-', 1)) AS lname
               FROM posts WHERE extra ? 'character' AND extra ? 'realm' AND extra->>'game_version' = 'retail'
               GROUP BY 1, 2, 3"""
        )
        conn.execute("ANALYZE posters")
        conn.execute(
            """INSERT INTO characters (region, realm, name)
               SELECT %(region)s, realm, name FROM posters WHERE name <> '' AND realm <> ''
               ON CONFLICT DO NOTHING""",
            {"region": REGION},
        )
        conn.execute(
            "INSERT INTO players (key) SELECT DISTINCT 'forum:' || author FROM posters ON CONFLICT DO NOTHING"
        )
        linked = conn.execute(
            """INSERT INTO player_characters (player_id, character_id, how)
               SELECT DISTINCT pl.id, ch.id, 'forum_alias'
               FROM posters ps
               JOIN players pl ON pl.key = 'forum:' || ps.author
               JOIN characters ch ON ch.region = %(region)s AND ch.realm = ps.realm AND lower(ch.name) = ps.lname
               ON CONFLICT DO NOTHING""",
            {"region": REGION},
        ).rowcount
    log.info("Linked %d new character(s) to forum accounts", linked)
    return linked


def current_season(conn) -> str | None:
    row = conn.execute(
        "SELECT slug FROM mplus_seasons WHERE starts <= now() AND (ends IS NULL OR ends > now()) "
        "ORDER BY starts DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def snapshot(conn, limit: int | None = None, delay: float = 1.2, **_) -> None:
    """Record this week's score and item level for every tracked character.

    Tracked = linked to a gold-labelled player, posted in a retail forum this season, or
    had a score this season at its last lookup. History is appended, never overwritten.
    """
    src = source_id(conn, SOURCE)
    fetcher = Fetcher(conn, src, delay=delay)
    try:
        _fetch_seasons(conn, fetcher)  # also refreshes the current season's cutoffs and history
        season = current_season(conn)
        link_forum_players(conn)
        targets = conn.execute(
            """WITH season_posters AS (  -- distinct characters that posted this season (one pass over posts)
                   SELECT DISTINCT realm_slug(extra->>'realm') AS realm,
                          lower(split_part(coalesce(extra->>'character', author), '-', 1)) AS lname
                   FROM posts
                   WHERE extra->>'game_version' = 'retail' AND extra ? 'realm'
                     AND created_at >= (SELECT starts FROM mplus_seasons WHERE slug = %(season)s)
               ), gold AS (
                   SELECT pc.character_id FROM player_characters pc JOIN players pl ON pl.id = pc.player_id
                   WHERE pl.key LIKE 'gold:%%'
               )
               SELECT ch.id, ch.realm, ch.name FROM characters ch
               LEFT JOIN season_posters sp ON sp.realm = ch.realm AND sp.lname = lower(ch.name)
               WHERE ch.region = %(region)s AND ch.found IS DISTINCT FROM false
                 AND NOT EXISTS (SELECT 1 FROM character_snapshots s
                                 WHERE s.character_id = ch.id AND s.taken_at > now() - interval '5 days')
                 AND (ch.id IN (SELECT character_id FROM gold)
                      OR coalesce((ch.mplus->>%(season)s)::numeric, 0) > 0
                      OR sp.realm IS NOT NULL)
               ORDER BY ch.id LIMIT %(limit)s""",
            {"region": REGION, "season": season, "limit": limit},
        ).fetchall()
        log.info("Snapshotting %d characters for %s", len(targets), season)
        taken = 0
        for i, (char_id, realm, name) in enumerate(targets, 1):
            raw_id, body = fetcher.get_json(
                f"{BASE}/characters/profile", "snapshot",
                params=_params(region=REGION, realm=realm, name=name,
                               fields=f"mythic_plus_scores_by_season:{season},gear,mythic_plus_best_runs:all"),
            )
            if not body:
                conn.execute("UPDATE characters SET found = false, looked_up_at = now() WHERE id = %s AND found IS NULL",
                             (char_id,))
                continue
            score = next((x.get("scores", {}).get("all") for x in body.get("mythic_plus_scores_by_season", [])
                          if x.get("season") == season), None)
            conn.execute(
                """INSERT INTO character_snapshots (character_id, season, score, item_level, runs, spec, raw_page_id)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (char_id, season, score, (body.get("gear") or {}).get("item_level_equipped"),
                 len(body.get("mythic_plus_best_runs") or []), body.get("active_spec_name"), raw_id),
            )
            taken += 1
            if i % 100 == 0:
                log.info("  %d/%d", i, len(targets))
        log.info("Done: %d snapshots taken", taken)
    finally:
        fetcher.close()
