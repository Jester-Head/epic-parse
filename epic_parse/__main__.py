"""Command line entry point: `python -m epic_parse <command>`.

  init-db                         create the database and tables
  fetch blizzard [options]        download forum data into raw_pages
  fetch raiderio [--limit N] [--workers N] [--alts]  look up forum posters' characters (or their alts) on Raider.IO
  fetch profiles [--limit N]      forum account profiles: every character on the account, About me
  fetch bnet [--limit N] [--workers N]  Blizzard Profile API: PvP, achievements, collections, stats
  fetch reddit [--category SUB]   WoW subreddits: new and changed threads, then parse and deletion check
  reddit-deletions [--days N]     blank Reddit posts deleted on Reddit (default: check everything)
  snapshot [--limit N]            weekly Raider.IO snapshot of tracked characters (first deletes expired Blizzard data)
  parse blizzard|raiderio         turn raw pages into rows
  import-v1 KIND PATH              load a data file from the v1 project (see importers/v1_archive.py)
  tag-patches [--overwrite]       tag forum posts with game version / expansion / patch
  projection [--season S] [--backtest]  projected end-of-season Mythic+ cutoffs
  milestones                      map keystone achievements; backfill milestone dates and best runs
  people                          rebuild the one-row-per-person tables and summarize them
  stats                           row counts per table
"""
import argparse
import logging
import sys
from typing import LiteralString

from psycopg import sql as pgsql

from epic_parse import db, milestones, people, projection, wow_patches
from epic_parse.sources.blizzard_api import Token
from epic_parse.importers import v1_archive
from epic_parse.sources import blizzard_api, blizzard_forums, forum_profiles, raiderio, reddit

SOURCES = {"blizzard": blizzard_forums, "bnet": blizzard_api, "profiles": forum_profiles, "raiderio": raiderio,
           "reddit": reddit}


def main() -> None:
    parser = argparse.ArgumentParser(prog="epic_parse", description="Collect WoW community data into Postgres.")
    parser.add_argument("-v", "--verbose", action="store_true", help="show debug logging")
    parser.add_argument("--log", metavar="FILE",
                        help="append all output (logging, prints, errors) to FILE; for windowless runs with pythonw")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="create the database (if needed) and apply db/schema.sql")

    p_fetch = sub.add_parser("fetch", help="download data from a source into raw_pages")
    p_fetch.add_argument("source", choices=SOURCES)
    p_fetch.add_argument("--category", action="append", dest="categories", metavar="SLUG",
                         help="forum category (e.g. gameplay) or, for reddit, subreddit (repeatable; default: the main ones)")
    p_fetch.add_argument("--max-pages", type=int, help="list pages per category (30 topics each; reddit: 100 posts each)")
    p_fetch.add_argument("--max-topics", type=int, help="stop after this many new/changed topics")
    p_fetch.add_argument("--limit", type=int, help="raiderio / profiles: max lookups this run")
    p_fetch.add_argument("--workers", type=int, help="raiderio / bnet: lookups to run in parallel (default 1 raiderio, 4 bnet)")
    p_fetch.add_argument("--retry-failed", action="store_true", default=None,
                         help="raiderio / bnet: also retry characters whose lookup failed after all retries before")
    p_fetch.add_argument("--alts", action="store_true", default=None,
                         help="raiderio: look up posters' other characters (from forum profiles) instead of the posters")
    p_fetch.add_argument("--refresh-days", type=int,
                         help="raiderio / bnet / profiles: re-fetch characters last fetched more than N days ago (default 30)")
    p_fetch.add_argument("--delay", type=float, help="seconds between requests (default: 1.5 forums and profiles, 1.2 raiderio)")

    p_parse = sub.add_parser("parse", help="turn unparsed raw pages into threads/posts rows")
    p_parse.add_argument("source", choices=SOURCES)

    p_import = sub.add_parser("import-v1", help="import a data file collected by the v1 project")
    p_import.add_argument("kind", choices=v1_archive.KINDS, help="what kind of file it is")
    p_import.add_argument("path", help="path to the file")

    p_tag = sub.add_parser("tag-patches", help="tag forum posts with game version, expansion and patch")
    p_tag.add_argument("--overwrite", action="store_true",
                       help="re-tag every post, not just untagged ones (e.g. after editing wow_patches.py)")

    p_snap = sub.add_parser("snapshot", help="weekly Raider.IO snapshot (score, item level) of tracked characters")
    p_snap.add_argument("--limit", type=int, help="max characters this run")

    p_proj = sub.add_parser("projection", help="projected end-of-season Mythic+ cutoff lines")
    p_proj.add_argument("--season", help="Raider.IO season slug (default: the current one)")
    p_proj.add_argument("--backtest", action="store_true", help="also show how accurate past projections were")

    sub.add_parser("milestones", help="map keystone achievements and backfill milestone dates and best runs")

    sub.add_parser("people", help="rebuild people / player_seasons (one row per person) and summarize")

    p_rdel = sub.add_parser("reddit-deletions", help="blank Reddit posts and comments deleted or removed on Reddit")
    p_rdel.add_argument("--days", type=int, help="only items from the last N days (default: everything)")

    sub.add_parser("stats", help="show row counts")

    args = parser.parse_args()
    if args.log:  # pythonw has no console, so send everything, tracebacks included, to the file
        sys.stdout = sys.stderr = open(args.log, "a", encoding="utf-8", buffering=1)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, stream=sys.stderr,
                        format="%(asctime)s %(levelname)-7s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S" if args.log else "%H:%M:%S")
    logging.getLogger("httpx").setLevel(logging.WARNING)

    if args.command == "init-db":
        db.init_db()
        return

    with db.connect() as conn:
        if args.command == "fetch":
            options = {k: v for k, v in vars(args).items()
                       if k in ("categories", "max_pages", "max_topics", "limit", "delay", "workers", "retry_failed",
                                "refresh_days", "alts") and v is not None}
            SOURCES[args.source].fetch(conn, **options)
        elif args.command == "parse":
            SOURCES[args.source].parse(conn)
        elif args.command == "import-v1":
            v1_archive.run(conn, args.kind, args.path)
        elif args.command == "snapshot":
            blizzard_api.purge_expired(conn)  # before snapshot(), which rebuilds people from what's left
            raiderio.snapshot(conn, limit=args.limit)
        elif args.command == "tag-patches":
            wow_patches.tag_posts(conn, overwrite=args.overwrite)
        elif args.command == "projection":
            projection.refresh(conn)
            season, rows = projection.report(conn, args.season)
            print(f"Projected end of {season} (US), from finished seasons at the same day:")
            print(f"  {'line':<9}{'day':>4}{'today':>9}{'projected':>11}{'low':>9}{'high':>9}{'seasons':>9}")
            for pct, day, cur, proj, low, high, n in rows:
                print(f"  top {100 - float(pct):<5g}{day:>4}{cur or 0:>9.0f}{proj or 0:>11.0f}{low or 0:>9.0f}{high or 0:>9.0f}{n:>9}")
            if args.backtest:
                print()
                print("Backtest (each finished season projected from the others):")
                print(f"  {'line':<9}{'week':>5}{'seasons':>9}{'median err':>12}{'worst err':>11}{'in range':>10}")
                for pct, week, n, med, worst, inside in projection.backtest(conn):
                    print(f"  top {100 - float(pct):<5g}{week:>5}{n:>9}{med:>11}%{worst:>10}%{inside:>9}%")
        elif args.command == "milestones":
            milestones.backfill(conn, Token())
        elif args.command == "people":
            people.refresh(conn)
            summaries: list[tuple[str, LiteralString]] = [
                ("people", "SELECT count(*), count(*) FILTER (WHERE characters > 0), count(*) FILTER (WHERE raiderio_found > 0) FROM people"),
                ("peak Mythic+ tier", "SELECT coalesce(peak_tier, 'unknown'), count(*) FROM people GROUP BY 1 ORDER BY 2 DESC"),
                ("current season tier (provisional)", "SELECT coalesce(current_tier, '(no score)'), count(*) FROM people GROUP BY 1 ORDER BY 2 DESC"),
            ]
            for label, query in summaries:
                print(f"{label}: {conn.execute(query).fetchall()}")
        elif args.command == "reddit-deletions":
            reddit.check_deletions(conn, days=args.days)
        elif args.command == "stats":
            for table in ("raw_pages", "threads", "posts"):
                count = db.scalar(conn, pgsql.SQL("SELECT count(*) FROM {}").format(pgsql.Identifier(table)))
                print(f"{table:<10} {count:>10,}")
            for name, threads, posts in conn.execute(
                "SELECT s.name, (SELECT count(*) FROM threads t WHERE t.source_id = s.id), "
                "(SELECT count(*) FROM posts p WHERE p.source_id = s.id) FROM sources s ORDER BY s.name"
            ):
                print(f"  {name:<16} {threads:>9,} threads {posts:>11,} posts")
            unparsed = db.scalar(conn, "SELECT count(*) FROM raw_pages WHERE parsed_at IS NULL "
                                       "AND kind IN ('topic', 'posts') AND status = 200")
            print(f"unparsed   {unparsed:>10,}")


if __name__ == "__main__":
    main()
