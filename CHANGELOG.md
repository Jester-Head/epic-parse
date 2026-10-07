# Changelog

All notable changes to Epic-Parse. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- End-of-season projections of the Mythic+ percentile lines (`python -m epic_parse projection
  [--backtest]`, SQL `mplus_cutoff_projection(season, percentile, date)`): today's value divided by
  the median share other finished seasons had reached by the same day, with a low-high range.
  Backtested leave-one-out: top-1% line typically within ~2% at weeks 2-6 and under 1% from week 10.
  DF Season 1 is left out of the model (thin Raider.IO archive). Raider.IO's site projection isn't
  in its public API.
- `snapshot_pace` view: each weekly snapshot with its same-day percentile and the projected final
  0.1/1/5/10% lines as seen that week, to compare where someone stood before they stopped playing
  with where the season was heading.
- Blizzard Profile API source (`fetch bnet`): for forum posters' retail characters, PvP ratings per
  bracket, honor level, achievement points and the date each achievement was earned, mount/pet/toy
  counts, lifetime statistics, raid kills with dates, item level and last login (`bnet_characters`,
  plus raw `bnet_*` pages). Credentials: `BLIZZARD_CLIENT_ID` / `BLIZZARD_CLIENT_SECRET` in `.env`.
- `Fetcher.get_json(transform=...)` to shrink very large responses before storing them
  (achievements are ~2 MB per character).
- Forum profiles source (`fetch profiles` / `parse profiles`): each forum account's public profile
  with every character on the Battle.net account (realm, class, race, level, achievement points,
  Classic flag), linked as `player_characters.how = 'account_alias'`, plus account stats and the
  "About me" text in the new `forum_accounts` table. Blizzard links these characters, so alts are
  verified rather than guessed.
- `characters.classic`, `level`, `achievement_points`. Characters flagged Classic are no longer
  looked up on Raider.IO (it only covers retail).
- Top-5% Mythic+ line for every season with Raider.IO curve data (`mplus_cutoffs` rows with
  `percentile = 95`, `derived = true`), interpolated from the season's score curve. Blizzard adds
  a top-5% reward in Midnight Season 3 (ranked per spec from then on; these lines are for all players).
- `mplus_score_at(season, percent [, at])`: the score needed for the top N% of a season, the
  reverse of `mplus_percentile`.
- `unknown_realms`: realms Raider.IO says don't exist in the US region (mostly Classic realms of
  players posting in retail forums). Characters on them are no longer looked up.

### Changed
- Raider.IO data now starts where Mythic+ did: Legion seasons 7.2–7.3.2 are included and forum
  posters are considered from patch 7.0.3 (2016-07-19). It previously started at BfA Season 1.
- Raider.IO lookups can run in parallel (`fetch raiderio --workers N`); Raider.IO often takes
  seconds to answer for characters it hasn't cached.
- The owner's characters and labels are ordinary self-reported data (`players.key = 'self:owner'`,
  `gold_labels.confidence = 'self_reported'`), not a gold standard. Weekly snapshots now track
  every self-reported character (e.g. survey respondents), not just gold-labelled players.

### Fixed
- Raider.IO's empty season-start points (score 0) are no longer stored in the cutoff history.
- A successful Raider.IO snapshot now marks the character as found and fills in class, spec, race,
  faction and the current-season score (1,181 characters were stuck as "not looked up").
- Seasons Raider.IO has no cutoffs for (BfA S1–4, Shadowlands S1–2) are no longer requested on
  every run (`mplus_seasons.has_cutoffs`).
- Realms parsed from forum usernames no longer keep a trailing account number
  (`wyrmrest-accord-3387509`); 1,551 posts corrected.

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
