# Epic Parse

This started a long time ago as a joke about simulating Barrens chat.
I’ve played World of Warcraft basically since it launched in 2004, and for a long time, the community has felt unheard.
“Blizzard doesn’t play their own game” is something I’m guilty of saying myself. So I wanted to see if I could
simulate having conversations with WoW players. My project gathers comments
from the WoW community, then uses them to build AI personas that respond to new features like different types of
players would. The personas are types of players, not copies of anyone, and no usernames are used.

## Status

- **Built:** the data foundation. Collection, parsing, and player context in PostgreSQL (below).
- **In progress:** turning that data into player personas, and checking their reactions against what
  real players said about past announcements.

Analysis of the data lives in [analysis-notebooks](https://github.com/Jester-Head/analysis-notebooks).

## How it works

Comments come from public WoW community discussions. Game data (Mythic+ seasons, cutoffs and more) comes
from public game APIs.

Every response is saved as-is in `raw_pages`. Parsers then turn the saved pages into tables, so a parser
can be fixed and re-run without downloading anything again.

## Features

- Stores community posts and comments, with full-text search
- Tags each post with its game version, expansion and patch
- Adds game context so comments can be grouped by type of player
- Takes weekly snapshots of Mythic+ scores
- Places scores on each season's percentile curve and projects end-of-season cutoffs
- Tracks keystone milestones and best runs across each season

## Setup

Requires Python and PostgreSQL.

```
pip install -e .
cp .env.example .env      # then fill in your database password and API keys
python -m epic_parse init-db
```

Run `python -m epic_parse --help` to see every command.

## Data and privacy

- Collected data stays in your own database. Nothing in this repo contains collected posts or player data.
- Usernames and other identifying details are never published or used in personas.

## History

The earlier MongoDB/Scrapy version is in the git tag `v1-archive`.

## License

MIT. See [LICENSE](LICENSE).
