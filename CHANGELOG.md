# Changelog

All notable changes to Epic-Parse. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Reddit source (`fetch reddit`): posts and comments from 16 WoW subreddits (retail, Classic and WoW
  Forever; list in `reddit.SUBREDDITS`) through Reddit's API (application-only OAuth). Threads are re-fetched when their comment
  count changes. Guardrails: about 40 requests a minute and a pause when Reddit's rate-limit header runs
  low; Reddit authors are never matched to forum accounts or characters; posts deleted or removed on
  Reddit are blanked (`reddit-deletions`, and the last 14 days on every fetch); Reddit raw pages are
  deleted after 7 days. Markdown is reduced to plain text and quoted lines go to `extra.quotes`.
- Milestone dates and best-run dates, to recover part of a character's history from before it was
  tracked (`python -m epic_parse milestones` backfills from stored responses):
  - `milestone_achievements`: Blizzard's season keystone achievements (Explorer, Conqueror, Master, Hero,
    Legend, Myth; BfA through Midnight, plus Legion's expansion-wide ones) mapped to Raider.IO seasons.
  - `character_milestones`: when each character's account earned each one, filled on every Blizzard
    lookup. Achievements are account-wide, so the date may come from another character; about 0.7% are
    dated after their season ended (account-wide copies).
  - `character_best_runs`: each dungeon's best run with its completion date, now requested on every
    Raider.IO lookup and snapshot. Every best ever seen is kept, so weekly snapshots record upgrades;
    runs that were later beaten before tracking started can't be recovered.
  - `player_seasons.milestone_dates` (earliest date per milestone across the person's characters) and
    `last_best_run_at`.
- Type checking: `pyright` (dev dependency) configured for the project venv; the code passes with
  no errors. `db.scalar()` returns a single value and fails clearly when a query returns no row.
- One row per person (`python -m epic_parse people`, also rebuilt by every weekly snapshot):
  - `people`: each forum account (or self-reported player) with forum activity, character counts
    (retail / Classic / found on Raider.IO / shared with another account), main character, Mythic+
    history (seasons played, best percentile, peak tier, current season), best PvP rating, honor
    level, achievement points, collections and last login. Account-wide values take the max over
    the person's characters.
  - `player_seasons`: one row per person per Mythic+ season with their best retail character,
    score, percentile, tier, elite flag (top 0.1%) and how many of their characters had a score.
  - `mplus_tier(season, score)`: fixed percentile bands so a tier is the same share of players every
    season: hardcore = top 1%, mid2 = top 5%, mid1 = top 20%, casual = any lower score; NULL for
    seasons without percentile data. (An earlier draft tied mid1 to Keystone Legend, but Legend
    ranged from about the top 15% to the top 29% between seasons.)
  - `mplus_milestones(season, score)`: keystone achievements a score qualifies for (Explorer through
    Myth) with each season's own thresholds, stored separately from the tier
    (`player_seasons.milestones` / `milestone`, `people.best_milestone`, `current_milestone`), so
    questions like "do people stop after Legend?" aren't built into the tier definition.
- Index on `player_characters (character_id)`.

### Removed
- The MIT license file. No license is granted; all rights reserved.
- An analysis script that didn't belong in this project, and its optional `matplotlib` dependency.

### Changed
- Blizzard API data is kept at most 30 days, as Blizzard's API terms require: `blizzard_api.purge_expired()`
  deletes older raw pages, `bnet_characters` rows and milestone dates (`character_milestones.fetched_at`),
  plus raw pages replaced by a newer fetch. It runs after every `fetch bnet` and before every weekly
  snapshot. A scheduled task re-fetches everyone every 4 weeks (`fetch bnet --refresh-days 21`), and a
  character Blizzard no longer returns has its stored details and milestone dates cleared.
- `fetch --refresh-days N` sets how old a lookup must be before it is repeated (raiderio, bnet, profiles).
- The keystone achievement map is refreshed on every `fetch bnet`.
- The HTTP user agent comes from `HTTP_USER_AGENT` in `.env` (default `EpicParse/0.2`) instead of the code.
- `fetch raiderio --alts` looks up the other characters on posters' accounts (forum profiles and
  self-reported) at level 45 and up, highest level first.
- `--log FILE` sends all output (logging, prints, tracebacks) to a file, so scheduled runs can use
  `pythonw` and open no console window. Log timestamps include the date in that mode.
- Lookups that still fail after all retries are skipped by later runs instead of retried every time
  (`characters.raiderio_failures` / `bnet_failures`, `forum_accounts.failures`). `fetch raiderio | bnet |
  profiles --retry-failed` includes them again; any real answer resets the count.
- README rewritten as a short overview: sources and features.

### Fixed
- Reddit reply linking refreshes table statistics first; with stale ones the first run's linking
  query ran for 12 hours.
- Rebuilding `people` computes each person-season's percentile once (`mplus_tier_for_pct`), about
  twice as fast; milestone and best-run columns are aggregated in one pass instead of per row.
- One failing character, account, thread or thread-list page no longer stops a whole run: the weekly
  snapshot, the forum profiles fetch and the forum crawl now log it and move on (the lookups already did).
- The weekly snapshot stops cleanly if Raider.IO lists no running season.
- Choosing characters to look up could run for hours: with a small `--limit` (or nothing left to do)
  Postgres picked a plan that re-scanned every post. Forum-poster candidates are now collected once into a
  temp table with statistics (`raiderio.load_candidates`).
- Type-checker findings: fetch status is read from the fetcher instead of re-queried; table and index
  names in SQL go through `sql.Identifier`; a forum topic-list page that fails no longer crashes the crawl
  (`data` could be `None`); corrected return types.

## [0.4.0] - 2026-10-06

Player data: who the forum posters are in the game, and how their Mythic+ seasons went.

### Added

**Raider.IO** (`fetch raiderio`, `snapshot`)
- Mythic+ seasons from Legion 7.2 through Midnight (`mplus_seasons`) and each season's percentile
  cutoffs (`mplus_cutoffs`: top 0.1/1/10/25/40%, plus an interpolated top 5%, marked `derived`).
- Full score curves per season (`mplus_percentile_points`: percentile lines, "timed all dungeons
  at +N" and keystone achievement thresholds) and how the lines moved day by day
  (`mplus_cutoff_history`).
- Character lookups for everyone who posted in a retail forum since patch 7.0.3, newest posters
  first: class, spec, race, faction, score for every season and raid progress (`characters`).
  Lookups can run in parallel (`--workers N`); a free app key (`RAIDERIO_KEY` in `.env`) raises
  the rate limit.
- Weekly snapshots of tracked characters' score, item level, runs and spec
  (`character_snapshots`), run by a Windows scheduled task ("Epic-Parse weekly Raider.IO
  snapshot", Tuesdays 9 AM). Raider.IO only keeps final scores for past seasons, so the weekly
  history is what shows when someone slowed down or stopped.
- `unknown_realms`: realms Raider.IO says don't exist in the US (mostly Classic realms); characters
  on them are skipped.

**Percentiles and projections** (SQL)
- `mplus_percentile(season, score [, date])`: share of players at or above a score, against the
  final distribution or the lines as they stood on a given date.
- `mplus_score_at(season, percent [, date])`: the reverse, e.g. the top-5% score.
- End-of-season projections of each line (`python -m epic_parse projection [--backtest]`,
  `mplus_cutoff_projection(season, percentile, date)`): today's value divided by the median share
  other finished seasons had reached by the same day, with a low-high range. Backtested
  leave-one-out: the top-1% line is typically within ~2% at weeks 2-6 and under 1% from week 10.
  DF Season 1 is left out of the model (thin Raider.IO archive). Raider.IO's own site projection
  isn't in its public API.
- `snapshot_pace` view: every weekly snapshot (including scores of 0) with its same-day
  percentile and the projected final 0.1/1/5/10% lines as seen that week.
- `season_rules` (rating system, Keystone Master requirement, key squishes per season; unverified
  fields left empty) and `season_events` (score-moving events like Turbo Boost).
- Blizzard adds a top-5% Mythic+ reward in Midnight Season 3, ranked per spec from then on; the
  lines here are for all players.

**Players and alts**
- `players`, `player_characters` and `gold_labels` tables. Each forum account is a player linked
  to the characters it posted as (`forum_alias`); people can also report their own characters
  (`self_reported`). `realm_slug()` matches realm names written different ways.
- Forum profiles source (`fetch profiles`): each forum account's public profile lists every
  character on its Battle.net account (realm, class, race, level, achievement points, Classic
  flag), linked as `account_alias`, so alts are verified rather than guessed. Account stats and
  the "About me" text go in `forum_accounts`.
- `characters.classic`, `level`, `achievement_points`; Classic characters are skipped on
  Raider.IO, which only covers retail.

**Blizzard Profile API** (`fetch bnet`)
- For forum posters' retail characters: PvP ratings per bracket, honor level, achievement points
  and the date each achievement was earned, mount/pet/toy counts, lifetime statistics, raid kills
  with dates, item level and last login (`bnet_characters`, plus raw `bnet_*` pages). Credentials:
  `BLIZZARD_CLIENT_ID` / `BLIZZARD_CLIENT_SECRET` in `.env`.
- `Fetcher.get_json(transform=...)` shrinks very large responses before storing them
  (achievements are ~2 MB per character; ~23 KB after).

**Analysis and research**
- Pilot survey (`docs/survey/`, **on hold**, not sent out): questions, consent text, codebook and a
  Google Apps Script that builds the form. Revised for neutral wording (no assumption that everyone pushes keys), standard
  survey structure, per-patch play-amount grids, separate unrated and rated PvP, and first-person
  project wording.

### Changed
- Raider.IO data starts where Mythic+ did: Legion seasons are included and forum posters are
  considered from patch 7.0.3 (2016-07-19). It previously started at BfA Season 1.
- The owner's characters and labels are ordinary self-reported data (`players.key = 'self:owner'`,
  `gold_labels.confidence = 'self_reported'`), not a gold standard. Weekly snapshots track every
  self-reported character (e.g. survey respondents).

### Fixed
- A successful Raider.IO snapshot marks the character as found and fills in class, spec, race,
  faction and the current-season score (1,181 characters were stuck as "not looked up").
- Seasons Raider.IO has no cutoffs for (Legion, BfA S1-4, Shadowlands S1-2) are no longer requested
  on every run (`mplus_seasons.has_cutoffs`).
- Raider.IO's empty season-start points (score 0) are no longer stored in the cutoff history.
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

[Unreleased]: https://github.com/Jester-Head/epic-parse/compare/12a5fb1...main
[0.4.0]: https://github.com/Jester-Head/epic-parse/compare/f8575c3...12a5fb1
[0.3.0]: https://github.com/Jester-Head/epic-parse/compare/ad9ef07...f8575c3
[0.2.0]: https://github.com/Jester-Head/epic-parse/compare/v1-archive...ad9ef07
[0.1.0]: https://github.com/Jester-Head/epic-parse/tree/v1-archive
