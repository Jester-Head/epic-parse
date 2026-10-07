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
import threading
from concurrent.futures import ThreadPoolExecutor

from epic_parse import people, projection
from epic_parse.db import connect, source_id
from epic_parse.fetch import Fetcher

log = logging.getLogger(__name__)

SOURCE = "raiderio"
BASE = "https://raider.io/api/v1"
REGION = "us"
EXPANSIONS = [6, 7, 8, 9, 10, 11]  # Legion (where Mythic+ began) through Midnight
MAIN_SEASON = re.compile(r"^season-((bfa|sl|df|tww|mn)-\d+|7\.\d+(\.\d+)?)$")  # Legion slugs are patch numbers
# ("season-7.3.2"); skips event and post-season variants like '-break-the-meta', 'season-post-legion'
PERCENTILES = {"p999": 99.9, "p990": 99.0, "p900": 90.0, "p750": 75.0, "p600": 60.0}
# Lines Raider.IO doesn't publish, interpolated from the season's curve (mplus_score_at).
# Top 5% is a Blizzard reward tier from Midnight Season 3 (ranked per spec from then on;
# these lines are for all players).
DERIVED_PERCENTILES = [95.0]
UNKNOWN_REALM = re.compile(r"^Failed to find realm (.+) in region")
# Skipped: characters on realms Raider.IO said don't exist, and characters the forum alias
# lists mark as Classic (Raider.IO only covers retail).
NOT_UNKNOWN_REALM = ("NOT EXISTS (SELECT 1 FROM unknown_realms u WHERE u.region = ch.region AND u.realm = ch.realm)"
                     " AND ch.classic IS NOT TRUE")
SINCE = "2016-07-19"  # patch 7.0.3, which added Mythic+ (Raider.IO scores start at Legion 7.2;
                      # it has no percentile cutoffs before Shadowlands S3)

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


def load_candidates(conn) -> int:
    """Put CANDIDATES_SQL's rows in a temp table `candidates` (with statistics) for this session.

    Joining the subquery directly let Postgres badly misjudge its size; with a LIMIT it chose a
    nested loop that re-scanned every post and could run for hours. Returns the row count.
    """
    conn.execute("DROP TABLE IF EXISTS candidates")
    n = conn.execute(f"CREATE TEMP TABLE candidates AS {CANDIDATES_SQL}", {"since": SINCE}).rowcount
    conn.execute("ANALYZE candidates")
    return n

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
    # Seasons Raider.IO has no cutoffs for (has_cutoffs = false) aren't asked again.
    for slug in conn.execute(
        "SELECT slug FROM mplus_seasons s WHERE ends > now() "
        "OR (has_cutoffs IS DISTINCT FROM false AND ("
        "    NOT EXISTS (SELECT 1 FROM mplus_cutoffs c WHERE c.season = s.slug AND NOT c.derived) "
        "    OR NOT EXISTS (SELECT 1 FROM mplus_percentile_points p WHERE p.season = s.slug))) ORDER BY starts"
    ).fetchall():
        raw_id, data = fetcher.get_json(f"{BASE}/mythic-plus/season-cutoffs", "cutoffs",
                                        params=_params(region=REGION, season=slug[0]))
        status = fetcher.last_status
        if status == 404:
            conn.execute("UPDATE mplus_seasons SET has_cutoffs = false WHERE slug = %s", (slug[0],))
            continue
        if data:
            conn.execute("UPDATE mplus_seasons SET has_cutoffs = true WHERE slug = %s", (slug[0],))
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
    store_derived_cutoffs(conn)
    projection.refresh(conn)
    return seasons


def store_derived_cutoffs(conn) -> None:
    """Interpolate DERIVED_PERCENTILES (e.g. top 5%) for every season with curve data."""
    for pct in DERIVED_PERCENTILES:
        conn.execute(
            """INSERT INTO mplus_cutoffs (season, percentile, min_score, population, derived)
               SELECT season, %(pct)s::numeric, mplus_score_at(season, 100 - %(pct)s::numeric),
                      round((100 - %(pct)s::numeric) / 100.0 * avg(population / fraction)), true
               FROM mplus_percentile_points WHERE fraction > 0 AND population > 0
               GROUP BY season HAVING mplus_score_at(season, 100 - %(pct)s::numeric) IS NOT NULL
               ON CONFLICT (season, percentile) DO UPDATE SET min_score = EXCLUDED.min_score,
                   population = EXCLUDED.population, derived = true, fetched_at = now()""",
            {"pct": pct},
        )


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
                 for pt in line.get("data", []) if pt.get("y")],  # y = 0 marks the empty season start
            )


def _record_not_found(conn, char_id: int, raw_id: int) -> None:
    """Mark a failed lookup. 'Failed to find realm X' marks the whole realm unknown."""
    msg = conn.execute("SELECT body->>'message' FROM raw_pages WHERE id = %s", (raw_id,)).fetchone()[0] or ""
    m = UNKNOWN_REALM.match(msg)
    if m:
        conn.execute("INSERT INTO unknown_realms (region, realm) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                     (REGION, m.group(1)))
        conn.execute("UPDATE characters SET found = false WHERE region = %s AND realm = %s AND found IS NULL",
                     (REGION, m.group(1)))
    conn.execute("UPDATE characters SET found = false, looked_up_at = now(), raw_page_id = %s, raiderio_failures = 0 "
                 "WHERE id = %s AND found IS NOT TRUE", (raw_id, char_id))


def _apply_profile(conn, char_id: int, raw_id: int, status: int | None, body: dict | None) -> bool:
    """Write one raw profile response onto its characters row. Returns found."""
    if status != 200 or not body:
        _record_not_found(conn, char_id, raw_id)
        return False
    mplus = {s["season"]: (s.get("scores") or {}).get("all", 0) for s in body.get("mythic_plus_scores_by_season", [])}
    raid = {
        slug: {k: p.get(k) for k in ("summary", "normal_bosses_killed", "heroic_bosses_killed",
                                      "mythic_bosses_killed", "total_bosses")}
        for slug, p in (body.get("raid_progression") or {}).items()
    }
    conn.execute(
        """UPDATE characters SET found = true, class = %s, spec = %s, race = %s, faction = %s,
               mplus = %s::jsonb, raid = %s::jsonb, looked_up_at = now(), raw_page_id = %s, raiderio_failures = 0
           WHERE id = %s""",
        (body.get("class"), body.get("active_spec_name"), body.get("race"), body.get("faction"),
         json.dumps(mplus), json.dumps(raid), raw_id, char_id),
    )
    return True


def fetch(conn, limit: int | None = None, refresh_days: int = 30, delay: float = 1.2, workers: int = 1,
          retry_failed: bool = False, **_) -> None:
    """Look up characters that posted in retail forums since SINCE, newest posters first.

    Characters looked up within `refresh_days` are skipped. `limit` caps lookups per run.
    `workers` > 1 runs that many lookups in parallel (each with its own connection and
    `delay`); Raider.IO can take seconds to answer for characters it hasn't cached.
    Characters whose lookup failed after all retries are skipped unless `retry_failed`.
    """
    src = source_id(conn, SOURCE)
    fetcher = Fetcher(conn, src, delay=delay)
    try:
        seasons = _fetch_seasons(conn, fetcher)
        fields = "mythic_plus_scores_by_season:" + ":".join(seasons) + \
                 ",raid_progression:current-expansion:previous-expansion"
        load_candidates(conn)
        conn.execute(
            f"""INSERT INTO characters (region, realm, name)
                SELECT %(region)s, realm, name FROM candidates c WHERE name <> '' AND realm <> ''
                ON CONFLICT DO NOTHING""",
            {"region": REGION, "since": SINCE},
        )
        todo = conn.execute(
            f"""SELECT ch.id, ch.realm, ch.name
                FROM characters ch JOIN candidates c ON c.realm = ch.realm AND lower(c.name) = lower(ch.name)
                WHERE ch.region = %(region)s AND {NOT_UNKNOWN_REALM}
                  AND (%(retry)s OR ch.raiderio_failures = 0)
                  AND (ch.looked_up_at IS NULL OR ch.looked_up_at < now() - make_interval(days => %(days)s))
                ORDER BY c.last_post DESC
                LIMIT %(limit)s""",
            {"region": REGION, "since": SINCE, "days": refresh_days, "limit": limit, "retry": retry_failed},
        ).fetchall()
    finally:
        fetcher.close()
    log.info("%d characters to look up with %d worker(s) (seasons: %s)", len(todo), workers, ", ".join(seasons))

    progress = {"done": 0, "found": 0, "errors": 0}
    lock = threading.Lock()

    def work(chunk):
        with connect() as wconn:
            wfetcher = Fetcher(wconn, src, delay=delay)
            try:
                for char_id, realm, name in chunk:
                    try:
                        raw_id, body = wfetcher.get_json(
                            f"{BASE}/characters/profile", "character",
                            params=_params(region=REGION, realm=realm, name=name, fields=fields))
                        status = wfetcher.last_status
                        ok = _apply_profile(wconn, char_id, raw_id, status, body)
                    except Exception as e:  # one bad character shouldn't stop the run
                        log.warning("Lookup failed for %s-%s, skipped from now on (--retry-failed): %s", name, realm, e)
                        wconn.execute("UPDATE characters SET raiderio_failures = raiderio_failures + 1 WHERE id = %s", (char_id,))
                        ok, failed = False, True
                    else:
                        failed = False
                    with lock:
                        progress["done"] += 1
                        progress["found"] += ok
                        progress["errors"] += failed
                        if progress["done"] % 100 == 0:
                            log.info("  %d/%d looked up, %d found, %d errors",
                                     progress["done"], len(todo), progress["found"], progress["errors"])
            finally:
                wfetcher.close()

    # Round-robin split keeps every worker on the newest posters first.
    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(work, [todo[i::workers] for i in range(workers)]))
    log.info("Done: %d/%d characters found on Raider.IO (%d errors)",
             progress["found"], len(todo), progress["errors"])


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


def _mark_found_from_snapshot(conn, char_id: int, season: str, score, body: dict) -> None:
    """A successful snapshot proves the character exists: fill in what the profile says.

    looked_up_at stays untouched, so the full lookup (all seasons, raids) still happens.
    """
    conn.execute(
        """UPDATE characters SET found = true,
               class = coalesce(%(class)s, class), spec = coalesce(%(spec)s, spec),
               race = coalesce(%(race)s, race), faction = coalesce(%(faction)s, faction),
               mplus = CASE WHEN %(score)s::numeric IS NULL THEN mplus
                            ELSE mplus || jsonb_build_object(%(season)s::text, %(score)s::numeric) END
           WHERE id = %(id)s""",
        {"id": char_id, "season": season, "score": score, "class": body.get("class"),
         "spec": body.get("active_spec_name"), "race": body.get("race"), "faction": body.get("faction")},
    )


def current_season(conn) -> str | None:
    row = conn.execute(
        "SELECT slug FROM mplus_seasons WHERE starts <= now() AND (ends IS NULL OR ends > now()) "
        "ORDER BY starts DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def snapshot(conn, limit: int | None = None, delay: float = 1.2, **_) -> None:
    """Record this week's score and item level for every tracked character.

    Tracked = characters that are self-reported (e.g. survey respondents) or belong to a gold-labelled
    player, that posted in a retail forum this season, or that had a score this season at their last
    lookup. History is appended, never overwritten.
    """
    src = source_id(conn, SOURCE)
    fetcher = Fetcher(conn, src, delay=delay)
    try:
        _fetch_seasons(conn, fetcher)  # also refreshes the current season's cutoffs and history
        season = current_season(conn)
        if season is None:
            log.warning("No running Mythic+ season on Raider.IO; skipping the snapshot")
            return
        link_forum_players(conn)
        targets = conn.execute(
            f"""WITH season_posters AS (  -- distinct characters that posted this season (one pass over posts)
                   SELECT DISTINCT realm_slug(extra->>'realm') AS realm,
                          lower(split_part(coalesce(extra->>'character', author), '-', 1)) AS lname
                   FROM posts
                   WHERE extra->>'game_version' = 'retail' AND extra ? 'realm'
                     AND created_at >= (SELECT starts FROM mplus_seasons WHERE slug = %(season)s)
               ), named AS (  -- characters people told us about, or that belong to gold-labelled players
                   SELECT pc.character_id FROM player_characters pc JOIN players pl ON pl.id = pc.player_id
                   WHERE pc.how = 'self_reported' OR pl.key LIKE 'gold:%%'
               )
               SELECT ch.id, ch.realm, ch.name FROM characters ch
               LEFT JOIN season_posters sp ON sp.realm = ch.realm AND sp.lname = lower(ch.name)
               WHERE ch.region = %(region)s AND ch.found IS DISTINCT FROM false AND {NOT_UNKNOWN_REALM}
                 AND ch.raiderio_failures = 0
                 AND NOT EXISTS (SELECT 1 FROM character_snapshots s
                                 WHERE s.character_id = ch.id AND s.taken_at > now() - interval '5 days')
                 AND (ch.id IN (SELECT character_id FROM named)
                      OR coalesce((ch.mplus->>%(season)s)::numeric, 0) > 0
                      OR sp.realm IS NOT NULL)
               ORDER BY ch.id LIMIT %(limit)s""",
            {"region": REGION, "season": season, "limit": limit},
        ).fetchall()
        log.info("Snapshotting %d characters for %s", len(targets), season)
        taken = 0
        for i, (char_id, realm, name) in enumerate(targets, 1):
            try:
                raw_id, body = fetcher.get_json(
                    f"{BASE}/characters/profile", "snapshot",
                    params=_params(region=REGION, realm=realm, name=name,
                                   fields=f"mythic_plus_scores_by_season:{season},gear,mythic_plus_best_runs:all"),
                )
            except Exception as e:  # one bad character shouldn't stop the weekly snapshot
                log.warning("Snapshot failed for %s-%s, skipped from now on (fetch --retry-failed): %s", name, realm, e)
                conn.execute("UPDATE characters SET raiderio_failures = raiderio_failures + 1 WHERE id = %s", (char_id,))
                continue
            if not body:
                _record_not_found(conn, char_id, raw_id)
                continue
            score = next((x.get("scores", {}).get("all") for x in body.get("mythic_plus_scores_by_season", [])
                          if x.get("season") == season), None)
            conn.execute(
                """INSERT INTO character_snapshots (character_id, season, score, item_level, runs, spec, raw_page_id)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (char_id, season, score, (body.get("gear") or {}).get("item_level_equipped"),
                 len(body.get("mythic_plus_best_runs") or []), body.get("active_spec_name"), raw_id),
            )
            _mark_found_from_snapshot(conn, char_id, season, score, body)
            taken += 1
            if i % 100 == 0:
                log.info("  %d/%d", i, len(targets))
        log.info("Done: %d snapshots taken", taken)
    finally:
        fetcher.close()
    people.refresh(conn)  # keep the one-row-per-person tables current every week
