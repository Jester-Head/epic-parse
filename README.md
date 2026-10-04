# Epic-Parse

Collects World of Warcraft community discussion into Postgres for analysis.
Sources so far: the official Blizzard WoW forums. Reddit, YouTube and Wowhead come next.

The original MongoDB/Scrapy version is preserved in the git tag `v1-archive`.

## How it works

1. **fetch** downloads pages from a source and stores every response untouched in `raw_pages`.
2. **parse** turns raw pages into clean `threads` and `posts` rows.

Because the raw data is kept, a parser can be fixed or extended and re-run without
scraping again. Fields every source shares are real columns; anything source-specific
(likes, class, realm, quotes, ...) goes in the `extra` JSONB column.

## Setup (Windows)

Requires Python 3.11+ and PostgreSQL (tested with 18).

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .
copy .env.example .env      # then put your Postgres password in .env
python -m epic_parse init-db
```

## Usage

```bash
# small test: 5 threads from one category
python -m epic_parse fetch blizzard --category gameplay --max-topics 5
python -m epic_parse parse blizzard
python -m epic_parse stats

# everything in the default categories (takes a long time; safe to stop and resume)
python -m epic_parse fetch blizzard
```

Re-running `fetch` skips threads that haven't had new posts since they were last fetched,
and only downloads posts that aren't stored yet.

Category slugs are the ones in forum URLs, e.g. `gameplay`, `classes`, `pvp`, `lore`,
`wow-classic`, or a subcategory such as `paladin` or `professions`. Realm forums (retail
and Classic) and the Off-Topic/Support/Recruitment categories are always skipped.

## Importing v1 data

Data collected by the old version can be loaded once per file:

```bash
python -m epic_parse import-v1 forum-log    path\to\spider.log            # forum posts logged by the v1 crawler
python -m epic_parse import-v1 youtube-json path\to\yt_comments.json      # raw YouTube API comment threads
python -m epic_parse import-v1 youtube-csv  path\to\comments.csv          # flattened YouTube comment exports
```

Imported rows are tagged `extra->>'v1_import' = 'true'`. If a live crawl later fetches
the same post, the crawled version replaces the imported one.

## Backups

`scripts\backup.ps1` dumps the database (compressed, about 1 GB), checks the dump is readable,
keeps the newest 4 in `%USERPROFILE%\backups\epic_parse`, and copies the newest to
`OneDrive\Backups\epic_parse\epic_parse_latest.dump`. A Windows scheduled task
("Epic-Parse weekly DB backup") runs it every Sunday at 3 AM, or at the next login if the
PC was off. Results are logged to `backup.log` in the backup folder.

```bash
# back up now
powershell -ExecutionPolicy Bypass -File scripts\backup.ps1

# restore into a fresh database (drop or rename the old one first)
"C:\Program Files\PostgreSQL\18\bin\pg_restore.exe" --create --dbname=postgres path\to\epic_parse_....dump
```

## Example queries

```sql
-- posts mentioning "nerf", newest first
SELECT t.title, p.author, p.created_at, left(p.body, 120)
FROM posts p JOIN threads t ON t.id = p.thread_id
WHERE p.body_tsv @@ plainto_tsquery('english', 'nerf')
ORDER BY p.created_at DESC;

-- most-liked posts by the poster's class
SELECT extra->>'class' AS class, count(*), avg((extra->>'likes')::int) AS avg_likes
FROM posts GROUP BY 1 ORDER BY 2 DESC;
```

## Layout

```
db/schema.sql                         tables: sources, raw_pages, threads, posts
epic_parse/db.py                      connection (.env) and init-db
epic_parse/fetch.py                   polite fetcher: delay, retries, saves to raw_pages
epic_parse/sources/blizzard_forums.py fetch + parse for the Blizzard forums
```
