# Epic-Parse

Collects World of Warcraft community data into PostgreSQL.

Sources:
- Blizzard WoW forums (threads, posts, and posters' public forum profiles)
- Raider.IO (Mythic+ scores, seasons and cutoffs)
- Blizzard Profile API (achievements, PvP, collections)

Every response is saved as-is in `raw_pages`. Parsers then turn the saved pages into tables, so a
parser can be fixed and re-run without downloading anything again.

## Features

- Crawls the forums and stores threads and posts, with full-text search
- Tags each post with its game version, expansion and patch
- Links forum accounts to their characters, and characters to their Raider.IO and Blizzard data
- Takes weekly snapshots of Mythic+ scores
- Places scores on each season's percentile curve and projects end-of-season cutoffs
- Records when keystone milestones were reached and when best runs were completed
- Combines each person's characters into one row per person and one row per person per season

The earlier MongoDB/Scrapy version is in the git tag `v1-archive`.
