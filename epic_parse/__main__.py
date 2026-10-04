"""Command line entry point: `python -m epic_parse <command>`.

  init-db                         create the database and tables
  fetch blizzard [options]        download forum data into raw_pages
  parse blizzard                  turn raw pages into threads/posts rows
  stats                           row counts per table
"""
import argparse
import logging

from epic_parse import db
from epic_parse.sources import blizzard_forums

SOURCES = {"blizzard": blizzard_forums}


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
    p_fetch.add_argument("--delay", type=float, default=1.5, help="seconds between requests (default 1.5)")

    p_parse = sub.add_parser("parse", help="turn unparsed raw pages into threads/posts rows")
    p_parse.add_argument("source", choices=SOURCES)

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
            SOURCES[args.source].fetch(conn, categories=args.categories, max_pages=args.max_pages,
                                       max_topics=args.max_topics, delay=args.delay)
        elif args.command == "parse":
            SOURCES[args.source].parse(conn)
        elif args.command == "stats":
            for table in ("raw_pages", "threads", "posts"):
                count = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                print(f"{table:<10} {count:>10,}")
            unparsed = conn.execute("SELECT count(*) FROM raw_pages WHERE parsed_at IS NULL "
                                    "AND kind IN ('topic', 'posts') AND status = 200").fetchone()[0]
            print(f"unparsed   {unparsed:>10,}")


if __name__ == "__main__":
    main()
