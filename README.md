# Epic-Parse

Collects World of Warcraft community data into PostgreSQL.

Sources:
- Blizzard WoW forums (threads, posts, and posters' public forum profiles)
- Raider.IO (Mythic+ scores, seasons and cutoffs)
- Blizzard Profile API (achievements, PvP, collections)

Every response is saved as-is in `raw_pages`. Parsers then turn the saved pages into tables, so a
parser can be fixed and re-run without downloading anything again.

## What it does

- Crawls the forums and stores threads and posts, with full-text search
- Tags each post with its game version, expansion and patch
- Links forum accounts to their characters, and characters to their Raider.IO and Blizzard data
- Takes weekly snapshots of Mythic+ scores
- Places scores on each season's percentile curve and projects end-of-season cutoffs
- Records when keystone milestones were reached and when best runs were completed
- Combines each person's characters into one row per person and one row per person per season

## Layout

```
db/schema.sql               tables, views and SQL functions
epic_parse/__main__.py      command line
epic_parse/db.py            database connection
epic_parse/fetch.py         HTTP fetcher (delay, retries, saves to raw_pages)
epic_parse/sources/         one module per source
epic_parse/importers/       v1 data importers
epic_parse/people.py        people and player_seasons
epic_parse/projection.py    Mythic+ cutoff projections
epic_parse/milestones.py    milestone and best-run dates
epic_parse/wow_patches.py   patch dates
scripts/backup.ps1          database backup
```

The earlier MongoDB/Scrapy version is in the git tag `v1-archive`.
