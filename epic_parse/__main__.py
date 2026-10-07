"""Command line entry point: `python -m epic_parse <command>`.

  init-db                         create the database and tables
  fetch blizzard [options]        download forum data into raw_pages
  fetch raiderio [--limit N] [--workers N]  look up forum posters' characters on Raider.IO
  fetch profiles [--limit N]      forum account profiles: every character on the account, About me
  snapshot [--limit N]            weekly Raider.IO snapshot of tracked characters
  parse blizzard|raiderio         turn raw pages into rows
  import-v1 KIND PATH              load a data file from the v1 project (see importers/v1_archive.py)
  tag-patches [--overwrite]       tag forum posts with game version / expansion / patch
  stats                           row counts per table
"""
import argparse
import logging

from epic_parse import db, wow_patches
from epic_parse.importers import v1_archive
from epic_parse.sources import blizzard_forums, forum_profiles, raiderio

SOURCES = {"blizzard": blizzard_forums, "profiles": forum_profiles, "raiderio": raiderio}


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
    p_fetch.add_argument("--workers", type=int, help="raiderio: lookups to run in parallel (default 1)")
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
