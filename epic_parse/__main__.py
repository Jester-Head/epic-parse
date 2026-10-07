"""Command line entry point: `python -m epic_parse <command>`.

  init-db                         create the database and tables
  fetch blizzard [options]        download forum data into raw_pages
  fetch raiderio [--limit N] [--workers N]  look up forum posters' characters on Raider.IO
  fetch profiles [--limit N]      forum account profiles: every character on the account, About me
  fetch bnet [--limit N] [--workers N]  Blizzard Profile API: PvP, achievements, collections, stats
  snapshot [--limit N]            weekly Raider.IO snapshot of tracked characters
  parse blizzard|raiderio         turn raw pages into rows
  import-v1 KIND PATH              load a data file from the v1 project (see importers/v1_archive.py)
  tag-patches [--overwrite]       tag forum posts with game version / expansion / patch
  projection [--season S] [--backtest]  projected end-of-season Mythic+ cutoffs
  people                          rebuild the one-row-per-person tables and summarize them
  stats                           row counts per table
"""
import argparse
import logging

from epic_parse import db, people, projection, wow_patches
from epic_parse.importers import v1_archive
from epic_parse.sources import blizzard_api, blizzard_forums, forum_profiles, raiderio

SOURCES = {"blizzard": blizzard_forums, "bnet": blizzard_api, "profiles": forum_profiles, "raiderio": raiderio}


def main() -> None:
    parser = argparse.ArgumentParser(prog="epic_parse", description="Collect WoW community data into Postgres.")
    parser.add_argument("-v", "--verbose", action="store_true", help="show debug logging")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="create the database (if needed) and apply db/schema.sql")

    p_fetch = sub.add_parser("fetch", help="download data from a source into raw_pages")
    p_fetch.add_argument("source", choices=SOURCES)
    p_fetch.add_argument("--category", action="append", dest="categories", metavar="SLUG",
                         help="category to crawl, e.g. gameplay (repeatable; default: the main WoW categories)")
    p_fetch.add_argument("--max-pages", type=int, help="topic-list pages per category (30 topics each)")
    p_fetch.add_argument("--max-topics", type=int, help="stop after this many new/changed topics")
    p_fetch.add_argument("--limit", type=int, help="raiderio / profiles: max lookups this run")
    p_fetch.add_argument("--workers", type=int, help="raiderio / bnet: lookups to run in parallel (default 1 raiderio, 4 bnet)")
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

    sub.add_parser("people", help="rebuild people / player_seasons (one row per person) and summarize")

    sub.add_parser("stats", help="show row counts")

    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("httpx").setLevel(logging.WARNING)

    if args.command == "init-db":
        db.init_db()
        return

    with db.connect() as conn:
        if args.command == "fetch":
            options = {k: v for k, v in vars(args).items()
                       if k in ("categories", "max_pages", "max_topics", "limit", "delay", "workers") and v is not None}
            SOURCES[args.source].fetch(conn, **options)
        elif args.command == "parse":
            SOURCES[args.source].parse(conn)
        elif args.command == "import-v1":
            v1_archive.run(conn, args.kind, args.path)
        elif args.command == "snapshot":
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
        elif args.command == "people":
            people.refresh(conn)
            for label, sql in [
                ("people", "SELECT count(*), count(*) FILTER (WHERE characters > 0), count(*) FILTER (WHERE raiderio_found > 0) FROM people"),
                ("peak Mythic+ tier", "SELECT coalesce(peak_tier, 'unknown'), count(*) FROM people GROUP BY 1 ORDER BY 2 DESC"),
                ("current season tier (provisional)", "SELECT coalesce(current_tier, '(no score)'), count(*) FROM people GROUP BY 1 ORDER BY 2 DESC"),
            ]:
                print(f"{label}: {conn.execute(sql).fetchall()}")
        elif args.command == "stats":
            for table in ("raw_pages", "threads", "posts"):
                count = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                print(f"{table:<10} {count:>10,}")
            for name, threads, posts in conn.execute(
                "SELECT s.name, (SELECT count(*) FROM threads t WHERE t.source_id = s.id), "
                "(SELECT count(*) FROM posts p WHERE p.source_id = s.id) FROM sources s ORDER BY s.name"
            ):
                print(f"  {name:<16} {threads:>9,} threads {posts:>11,} posts")
            unparsed = conn.execute("SELECT count(*) FROM raw_pages WHERE parsed_at IS NULL "
                                    "AND kind IN ('topic', 'posts') AND status = 200").fetchone()[0]
            print(f"unparsed   {unparsed:>10,}")


if __name__ == "__main__":
    main()
