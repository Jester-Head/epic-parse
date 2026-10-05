-- Epic-Parse schema. Safe to re-run: everything is CREATE ... IF NOT EXISTS.
--
-- Two layers:
--   raw_pages          every HTTP response, exactly as fetched (JSON in `body`)
--   threads / posts    clean rows produced by each source's parser from raw_pages
-- Re-running a parser never needs a re-scrape.

CREATE TABLE IF NOT EXISTS sources (
    id   serial PRIMARY KEY,
    name text NOT NULL UNIQUE              -- 'blizzard_forums', later 'reddit', 'youtube', ...
);

CREATE TABLE IF NOT EXISTS raw_pages (
    id         bigserial PRIMARY KEY,
    source_id  int NOT NULL REFERENCES sources,
    kind       text NOT NULL,              -- source-specific page type, e.g. 'topic', 'posts'
    url        text NOT NULL,
    status     int NOT NULL,               -- HTTP status code
    body       jsonb,
    fetched_at timestamptz NOT NULL DEFAULT now(),
    parsed_at  timestamptz                 -- NULL until a parser has processed it
);
CREATE INDEX IF NOT EXISTS raw_pages_unparsed ON raw_pages (source_id, kind) WHERE parsed_at IS NULL;
CREATE INDEX IF NOT EXISTS raw_pages_url ON raw_pages (url, fetched_at DESC);
-- Lets the fetcher quickly check "have I already got this version of this thread?"
CREATE INDEX IF NOT EXISTS raw_pages_topic_key ON raw_pages ((body->>'id'), (body->>'last_posted_at')) WHERE kind = 'topic';

CREATE TABLE IF NOT EXISTS threads (
    id             bigserial PRIMARY KEY,
    source_id      int NOT NULL REFERENCES sources,
    source_key     text NOT NULL,          -- the thread's ID on the original site
    category       text,                   -- forum section / subreddit / channel ...
    title          text,
    author         text,
    url            text,
    created_at     timestamptz,
    last_posted_at timestamptz,
    post_count     int,
    extra          jsonb NOT NULL DEFAULT '{}',   -- anything source-specific
    first_seen     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_id, source_key)
);
CREATE INDEX IF NOT EXISTS threads_category ON threads (category);

CREATE TABLE IF NOT EXISTS posts (
    id          bigserial PRIMARY KEY,
    source_id   int NOT NULL REFERENCES sources,
    source_key  text NOT NULL,             -- the post's ID on the original site
    thread_id   bigint REFERENCES threads,
    parent_id   bigint REFERENCES posts,   -- the post this one replies to, if known
    position    int,                       -- order within the thread (1 = opening post)
    author      text,
    body        text,                      -- cleaned plain text, quotes removed
    created_at  timestamptz,
    updated_at  timestamptz,
    raw_page_id bigint,                    -- the raw_pages row this came from (not a foreign key: see below)
    extra       jsonb NOT NULL DEFAULT '{}',   -- likes, class, race, quotes, ...
    body_tsv    tsvector GENERATED ALWAYS AS (to_tsvector('english', coalesce(body, ''))) STORED,
    UNIQUE (source_id, source_key)
);
-- posts.raw_page_id is provenance only. As a foreign key, every insert had to lock its
-- raw_pages row, which is a disk write; at millions of rows that made bulk imports crawl.
ALTER TABLE posts DROP CONSTRAINT IF EXISTS posts_raw_page_id_fkey;
CREATE INDEX IF NOT EXISTS posts_thread ON posts (thread_id, position);
CREATE INDEX IF NOT EXISTS posts_created ON posts (created_at);
CREATE INDEX IF NOT EXISTS posts_body_search ON posts USING gin (body_tsv);
CREATE INDEX IF NOT EXISTS posts_extra ON posts USING gin (extra jsonb_path_ops);

-- Files loaded by `python -m epic_parse import-v1 ...` (prevents loading the same file twice).
CREATE TABLE IF NOT EXISTS imports (
    id          bigserial PRIMARY KEY,
    source_id   int NOT NULL REFERENCES sources,
    path        text NOT NULL,
    kind        text NOT NULL,
    rows        int,
    finished_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (path, kind)
);
ALTER TABLE raw_pages ADD COLUMN IF NOT EXISTS import_id bigint REFERENCES imports;

-- ----------------------------------------------------------------------------
-- Player context from Raider.IO (raider.io). Their API terms: community and
-- personal use; anything public built on this data must link to raider.io.

-- Realm name -> slug: "Aman'Thul" -> 'amanthul', "Area 52" -> 'area-52'. Forum posts mix
-- display names (crawled) and slugs (v1 usernames); use this to match them to characters.
CREATE OR REPLACE FUNCTION realm_slug(text) RETURNS text
    LANGUAGE sql IMMUTABLE AS $$ SELECT lower(replace(replace(trim($1), '''', ''), ' ', '-')) $$;

-- Mythic+ seasons (main seasons only, not event or "break the meta" variants).
CREATE TABLE IF NOT EXISTS mplus_seasons (
    slug         text PRIMARY KEY,        -- e.g. 'season-mn-2'
    name         text,
    expansion_id int,
    starts       date,                    -- US region
    ends         date
);

-- Minimum score for each percentile of the season's player population (US).
CREATE TABLE IF NOT EXISTS mplus_cutoffs (
    season      text REFERENCES mplus_seasons,
    percentile  numeric,                  -- 99.9, 99, 90, 75, 60
    min_score   numeric,
    population  int,                      -- players at or above this score
    fetched_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, percentile)
);

-- Characters that posted, as looked up on Raider.IO.
CREATE TABLE IF NOT EXISTS characters (
    id           bigserial PRIMARY KEY,
    region       text NOT NULL DEFAULT 'us',
    realm        text NOT NULL,           -- realm_slug() of the realm in the post
    name         text NOT NULL,
    found        boolean,                 -- NULL until looked up; false = Raider.IO doesn't know it
    class        text,
    spec         text,
    race         text,
    faction      text,
    mplus        jsonb NOT NULL DEFAULT '{}',   -- {"season-mn-2": 3060.6, ...}
    raid         jsonb NOT NULL DEFAULT '{}',   -- {"raid-slug": {"summary": "7/8 H", "normal": 8, "heroic": 7, "mythic": 0}}
    looked_up_at timestamptz,
    raw_page_id  bigint
);
CREATE UNIQUE INDEX IF NOT EXISTS characters_key ON characters (region, realm, lower(name));

-- Score -> percentile curve for each season: every known (score, share of players at or
-- above it) point, from the percentile lines, "timed all dungeons at +N" and the keystone
-- achievements. Lets any percentile (e.g. top 5%) be interpolated.
CREATE TABLE IF NOT EXISTS mplus_percentile_points (
    season     text REFERENCES mplus_seasons,
    point      text,                      -- 'p990', 'allTimed20', 'keystoneLegend', ...
    min_score  numeric,
    fraction   numeric,                   -- share of the season's players at or above min_score
    population int,
    PRIMARY KEY (season, point)
);

-- How each percentile line moved during the season (Raider.IO graphData, ~daily).
-- Used to compare a score with the cutoff *on the same date*.
CREATE TABLE IF NOT EXISTS mplus_cutoff_history (
    season     text REFERENCES mplus_seasons,
    percentile numeric,                   -- 99.9, 99, 90, 75, 60
    at         timestamptz,
    min_score  numeric,
    players    int,                       -- players at or above the line at that time
    PRIMARY KEY (season, percentile, at)
);

-- Weekly snapshots of tracked characters, so peak vs final, active span and gear can be
-- measured (Raider.IO only keeps final scores for past seasons).
CREATE TABLE IF NOT EXISTS character_snapshots (
    character_id bigint REFERENCES characters,
    taken_at     timestamptz NOT NULL DEFAULT now(),
    season       text,
    score        numeric,
    item_level   numeric,                 -- equipped item level at snapshot time
    runs         int,                     -- dungeons with a scoring run this season
    spec         text,
    raw_page_id  bigint,
    PRIMARY KEY (character_id, taken_at)
);

-- Things that move scores for everyone (catch-up events, special weeks). Dates often come
-- from the community rather than official posts.
CREATE TABLE IF NOT EXISTS season_events (
    id      serial PRIMARY KEY,
    season  text,
    name    text NOT NULL,                -- e.g. 'Turbo Boost', 'Break the Meta'
    starts  date,
    ends    date,
    effect  text,                         -- what it changes
    source  text
);

-- What the rules were each season, so scores and words are read in their own era.
-- Raw scores are not comparable across eras (level squishes, scoring reworks, moving
-- achievement goalposts); compare percentiles within a season instead. NULL = not yet
-- verified. Which score achievements existed each season comes from Raider.IO's data
-- (mplus_percentile_points rows named 'keystone%'), not from this table.
CREATE TABLE IF NOT EXISTS season_rules (
    season          text PRIMARY KEY,     -- Raider.IO slug, e.g. 'season-bfa-2'
    rating_system   text,                 -- 'raiderio_score_only' (before Blizzard rating) or 'blizzard_rating'
    affix_split     boolean,              -- best Fortified/Tyrannical runs scored separately (150% / 50%)
    ksm_requirement text,                 -- what Keystone Master required that season
    key_squish      text,                 -- key-level squish introduced this season, if any
    notes           text,                 -- community context, e.g. how hard a level felt at the time
    sources         text
);
INSERT INTO season_rules (season, rating_system, affix_split, ksm_requirement, key_squish, sources) VALUES
    ('season-bfa-1', 'raiderio_score_only', NULL, 'all dungeons at +15 in time', NULL, 'wiki: Mythic+ Rating (replaced +5/+10/+15 achievements in 9.1.0)'),
    ('season-bfa-2', 'raiderio_score_only', NULL, 'all dungeons at +15 in time', NULL, 'wiki: Mythic+ Rating'),
    ('season-bfa-3', 'raiderio_score_only', NULL, 'all dungeons at +15 in time', NULL, 'wiki: Mythic+ Rating'),
    ('season-bfa-4', 'raiderio_score_only', NULL, 'all dungeons at +15 in time', NULL, 'wiki: Mythic+ Rating'),
    ('season-sl-1',  'raiderio_score_only', NULL, 'all dungeons at +15 in time', NULL, 'wiki: Mythic+ Rating'),
    ('season-sl-2',  'blizzard_rating', NULL, 'rating 2000', NULL, 'wiki: Mythic+ Rating (introduced 9.1.0, 2021-06-29)'),
    ('season-sl-3',  'blizzard_rating', NULL, 'rating 2000', NULL, 'wiki: Mythic+ Rating'),
    ('season-sl-4',  'blizzard_rating', NULL, 'rating 2000', NULL, 'wiki: Mythic+ Rating'),
    ('season-df-1',  'blizzard_rating', true, 'rating 2000', NULL, 'wiki: Mythic+ Rating (Fortified/Tyrannical split, DF S1-3)'),
    ('season-df-2',  'blizzard_rating', true, 'rating 2000', NULL, 'wiki: Mythic+ Rating'),
    ('season-df-3',  'blizzard_rating', true, 'rating 2000', NULL, 'wiki: Mythic+ Rating'),
    ('season-df-4',  'blizzard_rating', NULL, 'rating 2000', 'key levels squished (mapping not yet recorded)', 'wiki: Mythic+ Rating'),
    ('season-tww-1', 'blizzard_rating', false, 'rating 2000', NULL, 'wiki: Mythic+ Rating (single score from TWW)'),
    ('season-tww-2', 'blizzard_rating', false, 'rating 2000', NULL, 'wiki: Mythic+ Rating'),
    ('season-tww-3', 'blizzard_rating', false, 'rating 2000', NULL, 'wiki: Mythic+ Rating'),
    ('season-mn-1',  'blizzard_rating', false, 'rating 2000', NULL, 'wiki: Mythic+ Rating'),
    ('season-mn-2',  'blizzard_rating', false, 'rating 2000', NULL, 'wiki: Mythic+ Rating')
ON CONFLICT (season) DO NOTHING;   -- never overwrite rows edited by hand

-- A player is one person behind one or more characters: a forum account, or a person who
-- told us their characters.
CREATE TABLE IF NOT EXISTS players (
    id         bigserial PRIMARY KEY,
    key        text NOT NULL UNIQUE,      -- 'forum:<account username>' or 'gold:<label>'
    notes      text,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS player_characters (
    player_id    bigint REFERENCES players,
    character_id bigint REFERENCES characters,
    how          text NOT NULL,           -- 'forum_alias' (posted as it) or 'self_reported'
    role         text,                    -- e.g. 'main this season', 'occasional alt'
    PRIMARY KEY (player_id, character_id)
);

-- Human-confirmed labels: the ground truth classifiers are tested against.
-- season NULL = applies to the player in general.
CREATE TABLE IF NOT EXISTS gold_labels (
    id         bigserial PRIMARY KEY,
    player_id  bigint REFERENCES players,
    season     text,
    trait      text NOT NULL,             -- trait_schema name, e.g. 'commitment_tier', 'social_mode'
    value      text NOT NULL,
    confidence text NOT NULL DEFAULT 'confirmed',   -- 'confirmed', 'boundary', 'needs_review'
    note       text,
    labeled_by text NOT NULL,
    labeled_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO sources (name) VALUES ('blizzard_forums'), ('youtube'), ('raiderio') ON CONFLICT DO NOTHING;
