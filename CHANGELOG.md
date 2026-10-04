# Changelog

All notable changes to Epic-Parse. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/).

## [0.3.0] - 2026-10-04

### Added
- Patch tagging: every forum post gets `game_version` (retail / classic / forever), `expansion`
  and, for retail, `patch` in `posts.extra`, based on the post date and forum category.
  New crawls are tagged automatically by `parse`; `python -m epic_parse tag-patches [--overwrite]`
  tags existing posts. Patch dates live in `epic_parse/wow_patches.py`.
- Retail patch table covers 3.3.0 (2009) through 12.1.0 (Midnight, 2026), with dates checked
  against warcraft.wiki.gg.
- The renamed Classic progression forum ("Mists of Pandaria Classic Discussion", formerly the
  Burning Crusade / Wrath / Cataclysm Classic forum) is tagged by date with the Classic
  expansion that was live at the time.
- This changelog.

### Changed
- All 533k imported v1 forum posts were re-tagged with the new table. v1 marked posts before
  August 2018 and after June 2025 as "Unknown", had wrong dates for 11.0.0, 11.0.2 and 10.2.7,
  and tagged the whole progression forum as one Classic expansion. The original v1 values are
  still in `raw_pages`.
- The bulk-load helper (drop and rebuild search indexes) moved to `epic_parse/db.py` so the
  importer and the tagger share it.

## [0.2.0] - 2026-10-04

Restart of the project on PostgreSQL. The v1 code is preserved in git tag `v1-archive`.

### Added
- PostgreSQL schema (`db/schema.sql`): `sources`, `raw_pages` (every response, untouched),
  `threads` and `posts` (cleaned rows, shared by all sources), `imports`. Source-specific fields go
  in a `jsonb extra` column; posts have an indexed full-text search column (`body_tsv`).
- Command line: `python -m epic_parse init-db | fetch | parse | import-v1 | stats`.
- Blizzard forums source (`epic_parse/sources/blizzard_forums.py`) using the Discourse JSON API,
  with separate fetch and parse steps:
  - every post of a thread is fetched (stream of post IDs, batches of 20);
  - threads with no new posts are skipped and already-stored posts aren't re-downloaded;
  - polite fetching: 1.5 s between requests, retries on 429/5xx, identifying User-Agent;
  - structured quotes (quoted user, post number, text), reply links (`parent_id`), thread
    position, character, realm (falling back to the username), guild, level, trust level,
    edit count, deleted/hidden flags.
- All realm forums (retail and Classic, via the forum's `is_realm` flag) are skipped, along
  with Off-Topic, Support, Recruitment and similar categories.
- v1 importers (`import-v1 forum-log | youtube-json | youtube-csv`): loaded 533,007 forum posts
  (2011–2025) recovered from the v1 crawler log, and 3,215,245 YouTube comments on 24,660 videos
  merged from the raw API export and two CSV exports. Imported rows are tagged `v1_import`; a live
  crawl replaces them, never the reverse.
- Weekly backups (`scripts/backup.ps1` + Windows scheduled task): compressed `pg_dump`, verified,
  newest 4 kept locally, newest copied to OneDrive.
- README with setup, usage, import, backup and example queries.

### Changed
- Replaced Scrapy with plain Python (`httpx` + `psycopg`).
- Replaced MongoDB with PostgreSQL 18 (native Windows install).

### Fixed
- Post text no longer glues paragraphs together (v1 used `get_text(strip=True)`).
- Bulk imports: `posts.raw_page_id` is no longer a foreign key (each insert locked its raw row,
  causing millions of extra writes); reply linking joins on indexed columns.

### Removed
- Secrets from version control: the Reddit credentials file is git-ignored, a Hugging Face token
  was removed from a notebook, and the database password lives in a git-ignored `.env`.

### Not yet ported from v1
- Live YouTube collection (only historical data is imported), Reddit scrapers, Wowhead scraper,
  and the analysis notebooks (still in the local `_v1/` folder and the `v1-archive` tag).

## [0.1.0] - 2025-07 (`v1-archive`)

The original version: Scrapy spider for the Blizzard forums, YouTube API comment scraper, Reddit
and Wowhead scrapers, MongoDB storage, and Jupyter notebooks for jargon and sentiment analysis.

[0.3.0]: https://github.com/Jester-Head/epic-parse/compare/v1-archive...main
[0.2.0]: https://github.com/Jester-Head/epic-parse/compare/v1-archive...main
[0.1.0]: https://github.com/Jester-Head/epic-parse/tree/v1-archive
