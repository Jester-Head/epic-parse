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

-- Share of a season's players (in %) with a score at or above p_score: 1.8 means "top 1.8%".
-- Without p_at: against the final distribution (all known curve points).
-- With p_at: against the percentile lines as they stood on that date (cutoff history), so a
-- score from week 6 is compared with week 6, not with the end of the season.
-- Log-linear interpolation between known points. Above the highest known point it returns
-- that point's share (an upper bound); below the lowest, that point's share (a lower bound).
-- NULL when the season has no cutoff data or the score is 0.
CREATE OR REPLACE FUNCTION mplus_percentile(p_season text, p_score numeric, p_at timestamptz DEFAULT NULL)
RETURNS numeric LANGUAGE sql STABLE AS $$
    WITH pts AS (
        SELECT min_score AS s, fraction AS f FROM mplus_percentile_points
        WHERE season = p_season AND p_at IS NULL AND fraction > 0
        UNION ALL
        SELECT * FROM (
            SELECT DISTINCT ON (percentile) min_score, (100 - percentile) / 100.0
            FROM mplus_cutoff_history
            WHERE season = p_season AND p_at IS NOT NULL AND at <= p_at
            ORDER BY percentile, at DESC
        ) h
    ),
    lo AS (SELECT s, f FROM pts WHERE s <= p_score ORDER BY s DESC LIMIT 1),
    hi AS (SELECT s, f FROM pts WHERE s >= p_score ORDER BY s ASC LIMIT 1)
    SELECT round(100 * CASE
        WHEN coalesce(p_score, 0) <= 0 THEN NULL
        WHEN NOT EXISTS (SELECT 1 FROM hi) THEN (SELECT f FROM lo)
        WHEN NOT EXISTS (SELECT 1 FROM lo) THEN (SELECT f FROM hi)
        WHEN (SELECT s FROM hi) = (SELECT s FROM lo) THEN (SELECT f FROM lo)
        ELSE exp(ln((SELECT f FROM lo)) + (p_score - (SELECT s FROM lo)) / ((SELECT s FROM hi) - (SELECT s FROM lo))
                 * (ln((SELECT f FROM hi)) - ln((SELECT f FROM lo))))
    END, 3)
$$;

-- The reverse of mplus_percentile: the score needed to be in the top p_pct percent of a
-- season (e.g. 5 -> the top-5% line). Same points and log-linear interpolation; with p_at,
-- uses the percentile lines as they stood on that date (only 0.1/1/10/25/40% exist there,
-- so in-between values are coarser). NULL if the season has no curve data or p_pct is
-- outside the known points.
CREATE OR REPLACE FUNCTION mplus_score_at(p_season text, p_pct numeric, p_at timestamptz DEFAULT NULL)
RETURNS numeric LANGUAGE sql STABLE AS $$
    WITH pts AS (
        SELECT min_score AS s, fraction AS f FROM mplus_percentile_points
        WHERE season = p_season AND p_at IS NULL AND fraction > 0
        UNION ALL
        SELECT * FROM (
            SELECT DISTINCT ON (percentile) min_score, (100 - percentile) / 100.0
            FROM mplus_cutoff_history
            WHERE season = p_season AND p_at IS NOT NULL AND at <= p_at
            ORDER BY percentile, at DESC
        ) h
    ),
    t AS (SELECT p_pct / 100.0 AS f),
    -- round(.., 5): Raider.IO's lines sit a hair off their nominal share (0.1% is 0.1002%)
    lo AS (SELECT s, f FROM pts WHERE round(f, 5) >= round((SELECT f FROM t), 5) ORDER BY f ASC, s DESC LIMIT 1),  -- lower score side
    hi AS (SELECT s, f FROM pts WHERE round(f, 5) <= round((SELECT f FROM t), 5) ORDER BY f DESC, s ASC LIMIT 1)   -- higher score side
    SELECT round(CASE
        WHEN NOT EXISTS (SELECT 1 FROM lo) OR NOT EXISTS (SELECT 1 FROM hi) THEN NULL
        WHEN (SELECT f FROM hi) = (SELECT f FROM lo) THEN (SELECT s FROM lo)
        ELSE (SELECT s FROM lo) + (ln((SELECT f FROM t)) - ln((SELECT f FROM lo)))
             / (ln((SELECT f FROM hi)) - ln((SELECT f FROM lo))) * ((SELECT s FROM hi) - (SELECT s FROM lo))
    END, 2)
$$;

-- Projection model: how far along each percentile line was on each day of a finished season,
-- as a share of its final value (share = value / final). Built by epic_parse.projection.refresh()
-- from the cutoff history; the 95 (top 5%) line is interpolated with mplus_score_at.
CREATE TABLE IF NOT EXISTS mplus_line_progress (
    season     text,
    percentile numeric,
    day        int,                       -- days since the season started (US)
    value      numeric,
    final      numeric,
    share      numeric,
    PRIMARY KEY (season, percentile, day)
);

-- Projected end-of-season value of a percentile line (99.9, 99, 95, 90, 75, 60), as seen on p_at:
-- the line's value that day divided by the median share other finished seasons had reached by
-- the same day. low/high use the extreme shares. NULL projection with fewer than 3 seasons.
-- The season itself is never part of its own model, so past seasons can be backtested.
CREATE OR REPLACE FUNCTION mplus_cutoff_projection(p_season text, p_percentile numeric, p_at timestamptz DEFAULT now())
RETURNS TABLE (day int, current_score numeric, projected numeric, low numeric, high numeric, seasons_used int)
LANGUAGE sql STABLE AS $$
    WITH d AS (SELECT (p_at::date - starts) AS day FROM mplus_seasons WHERE slug = p_season),
    cur AS (SELECT mplus_score_at(p_season, 100 - p_percentile, p_at) AS v),
    r AS (
        SELECT lp.share FROM mplus_line_progress lp, d
        WHERE lp.percentile = p_percentile AND lp.day = d.day AND lp.season <> p_season AND lp.share > 0
    )
    SELECT d.day, cur.v,
           CASE WHEN count(r.share) >= 3 THEN round(cur.v / (percentile_cont(0.5) WITHIN GROUP (ORDER BY r.share))::numeric, 1) END,
           CASE WHEN count(r.share) >= 3 THEN round(cur.v / max(r.share), 1) END,
           CASE WHEN count(r.share) >= 3 THEN round(cur.v / min(r.share), 1) END,
           count(r.share)::int
    FROM d CROSS JOIN cur LEFT JOIN r ON true
    GROUP BY d.day, cur.v
$$;

-- Raider.IO publishes 99.9/99/90/75/60 lines only. Extra lines (e.g. top 5%, a Blizzard
-- reward tier from Midnight Season 3) are interpolated with mplus_score_at and marked derived.
ALTER TABLE mplus_cutoffs ADD COLUMN IF NOT EXISTS derived boolean NOT NULL DEFAULT false;

-- false = Raider.IO has no cutoff data for the season (seasons before Shadowlands S3);
-- such seasons aren't requested again.
ALTER TABLE mplus_seasons ADD COLUMN IF NOT EXISTS has_cutoffs boolean;

-- Realms Raider.IO says don't exist in the region (mostly Classic realms of players posting
-- in retail forums). Characters on them are marked found = false and never looked up.
CREATE TABLE IF NOT EXISTS unknown_realms (
    region     text NOT NULL,
    realm      text NOT NULL,
    first_seen timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (region, realm)
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
    -- Legion: Raider.IO score only; KSM rules not verified yet (NULL)
    ('season-7.2.0', 'raiderio_score_only', NULL, NULL, NULL, 'Raider.IO static-data (expansion 6)'),
    ('season-7.2.5', 'raiderio_score_only', NULL, NULL, NULL, 'Raider.IO static-data (expansion 6)'),
    ('season-7.3.0', 'raiderio_score_only', NULL, NULL, NULL, 'Raider.IO static-data (expansion 6)'),
    ('season-7.3.2', 'raiderio_score_only', NULL, NULL, NULL, 'Raider.IO static-data (expansion 6)'),
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
    key        text NOT NULL UNIQUE,      -- 'forum:<account username>', 'self:<label>' (told us their
                                          -- characters, e.g. survey) or 'gold:<label>' (human-verified)
    notes      text,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS player_characters (
    player_id    bigint REFERENCES players,
    character_id bigint REFERENCES characters,
    how          text NOT NULL,           -- 'forum_alias' (posted as it), 'account_alias' (listed on the
                                          -- forum account's profile but not posted as) or 'self_reported'
    role         text,                    -- e.g. 'main this season', 'occasional alt'
    PRIMARY KEY (player_id, character_id)
);
CREATE INDEX IF NOT EXISTS player_characters_character ON player_characters (character_id);

-- Human-confirmed labels: the ground truth classifiers are tested against.
-- season NULL = applies to the player in general.
CREATE TABLE IF NOT EXISTS gold_labels (
    id         bigserial PRIMARY KEY,
    player_id  bigint REFERENCES players,
    season     text,
    trait      text NOT NULL,             -- trait_schema name, e.g. 'commitment_tier', 'social_mode'
    value      text NOT NULL,
    confidence text NOT NULL DEFAULT 'confirmed',   -- 'confirmed', 'boundary', 'needs_review', or
                                                    -- 'self_reported' (the player's own answer, not verified)
    note       text,
    labeled_by text NOT NULL,
    labeled_at timestamptz NOT NULL DEFAULT now()
);

-- Public profiles of forum accounts (forum_profiles source). Account stats, the "About me"
-- text, and (via player_characters how = 'account_alias') every character on the account.
CREATE TABLE IF NOT EXISTS forum_accounts (
    username        text PRIMARY KEY,         -- forum username, e.g. 'WhovianIV-2296788'
    found           boolean,                  -- false = profile gone (404)
    user_id         bigint,
    display_name    text,
    bio             text,                     -- "About me" as plain text, if filled in
    account_created timestamptz,
    last_posted_at  timestamptz,
    last_seen_at    timestamptz,
    post_count      int,
    time_read_s     bigint,                   -- seconds spent reading the forums
    profile_views   int,
    alias_count     int,                      -- characters listed on the account
    fetched_at      timestamptz,
    raw_page_id     bigint
);

-- From the forum alias lists: whether a character is on Classic, its level and achievement
-- points. Raider.IO only covers retail, so classic = true characters aren't looked up there.
ALTER TABLE characters ADD COLUMN IF NOT EXISTS classic boolean;
ALTER TABLE characters ADD COLUMN IF NOT EXISTS level int;
ALTER TABLE characters ADD COLUMN IF NOT EXISTS achievement_points int;

-- Blizzard Profile API (blizzard_api source): what Raider.IO doesn't have. One row per looked-up
-- character; achievements (with dates), statistics and raid kills stay in raw_pages
-- (kinds 'bnet_achievements', 'bnet_statistics', 'bnet_raids').
CREATE TABLE IF NOT EXISTS bnet_characters (
    character_id           bigint PRIMARY KEY REFERENCES characters,
    found                  boolean,           -- false = 404: long inactive, very low level, or renamed/moved
    level                  int,
    last_login             timestamptz,
    item_level             int,               -- equipped
    achievement_points     int,
    achievements_completed int,
    guild                  text,
    active_spec            text,
    honor_level            int,
    honorable_kills        int,
    pvp                    jsonb NOT NULL DEFAULT '{}',  -- {"ARENA_3v3": {"rating": 2104, "played": 210, "won": 118, "season": 40}, ...}
    mounts                 int,
    pets                   int,
    toys                   int,
    fetched_at             timestamptz,
    raw_page_id            bigint             -- the summary response
);

-- Each weekly snapshot next to where the season stood and where it was heading that week:
-- same-day percentile, and the projected end-of-season 0.1/1/5/10% lines as seen on that date.
-- For someone who stopped playing, compare their last score with these to see how they'd have
-- ranked had the season ended then vs where it was going. Snapshots with a score of 0 (no Mythic+
-- this season yet, or at all) are included; their pct_same_day is NULL.
CREATE OR REPLACE VIEW snapshot_pace AS
SELECT s.character_id, ch.name, ch.realm, s.season, s.taken_at, s.score, s.item_level,
       mplus_percentile(s.season, s.score, s.taken_at)            AS pct_same_day,
       (SELECT projected FROM mplus_cutoff_projection(s.season, 99.9, s.taken_at)) AS proj_top_0_1,
       (SELECT projected FROM mplus_cutoff_projection(s.season, 99,   s.taken_at)) AS proj_top_1,
       (SELECT projected FROM mplus_cutoff_projection(s.season, 95,   s.taken_at)) AS proj_top_5,
       (SELECT projected FROM mplus_cutoff_projection(s.season, 90,   s.taken_at)) AS proj_top_10
FROM character_snapshots s JOIN characters ch ON ch.id = s.character_id;

-- Mythic+ commitment tier for a season score, by fixed percentile bands so a tier means the same
-- share of players every season: hardcore = top 1% (elite = top 0.1%, flagged separately),
-- mid2 = top 5%, mid1 = top 20%, casual = any lower score, none = no score. NULL when the season
-- has no percentile data (Legion, BfA, Shadowlands S1-2). Milestones like Keystone Legend are kept
-- separately (mplus_milestones): their difficulty varied between seasons (Legend was ~top 15% to
-- ~top 29%), and keeping them out of the tier lets "do people stop after Legend?" be asked directly.
CREATE OR REPLACE FUNCTION mplus_tier(p_season text, p_score numeric) RETURNS text
LANGUAGE sql STABLE AS $$
    SELECT CASE
        WHEN coalesce(p_score, 0) <= 0 THEN 'none'
        WHEN x.pct IS NULL THEN NULL
        WHEN x.pct <= 1 THEN 'hardcore'
        WHEN x.pct <= 5 THEN 'mid2'
        WHEN x.pct <= 20 THEN 'mid1'
        ELSE 'casual'
    END
    FROM (SELECT mplus_percentile(p_season, p_score) AS pct) x
$$;

-- Keystone achievements a season score qualifies for (Explorer, Conqueror, Master, Hero, Legend,
-- Myth), using Raider.IO's thresholds for that season, lowest first. Empty array = none reached;
-- NULL = no threshold data for the season (before Shadowlands S3).
CREATE OR REPLACE FUNCTION mplus_milestones(p_season text, p_score numeric) RETURNS text[]
LANGUAGE sql STABLE AS $$
    SELECT CASE
        WHEN NOT EXISTS (SELECT 1 FROM mplus_percentile_points WHERE season = p_season AND point LIKE 'keystone%') THEN NULL
        ELSE coalesce((SELECT array_agg(replace(point, 'keystone', 'Keystone ') ORDER BY min_score)
                       FROM mplus_percentile_points
                       WHERE season = p_season AND point LIKE 'keystone%' AND p_score >= min_score), '{}')
    END
$$;

-- One row per person per Mythic+ season they have a score in (rebuilt by epic_parse.people.refresh).
-- A person's season = their best retail character that season; alts with a score are counted.
-- pct is against the season's final distribution (for the running season: as it stands now).
CREATE TABLE IF NOT EXISTS player_seasons (
    player_id             bigint,
    season                text,
    best_character_id     bigint,
    best_character        text,                 -- 'Name-realm'
    class                 text,
    score                 numeric,
    pct                   numeric,              -- top N% (of characters with a score)
    tier                  text,                 -- mplus_tier()
    elite                 boolean,              -- top 0.1%
    characters_with_score int,
    provisional           boolean,              -- season still running
    PRIMARY KEY (player_id, season)
);
ALTER TABLE player_seasons ADD COLUMN IF NOT EXISTS milestones text[];   -- mplus_milestones(): all reached
ALTER TABLE player_seasons ADD COLUMN IF NOT EXISTS milestone text;      -- the highest one reached

-- One row per person: a forum account (or self-reported player) with everything known about
-- them across their characters and sources (rebuilt by epic_parse.people.refresh).
CREATE TABLE IF NOT EXISTS people (
    player_id           bigint PRIMARY KEY,
    key                 text,
    forum_username      text,
    -- forum
    account_created     timestamptz,
    forum_post_count    int,                    -- Blizzard's lifetime count
    posts_collected     int,                    -- posts in this database
    first_post          timestamptz,
    last_post           timestamptz,
    retail_post_share   numeric,                -- share of collected posts in retail forums
    top_category        text,
    -- characters
    characters          int,
    retail_characters   int,
    classic_characters  int,
    raiderio_found      int,
    shared_characters   int,                    -- also linked to another account (rename/transfer?)
    main_character      text,                   -- best character in their latest M+ season, else highest level/achievements
    main_class          text,
    -- Mythic+ (Raider.IO)
    mplus_seasons       int,                    -- seasons with a score
    first_mplus_season  text,
    last_mplus_season   text,
    best_pct            numeric,
    best_pct_season     text,
    peak_tier           text,                   -- best tier any season; 'none' = no M+ score, 'no_pct_data' = only
                                                -- scores in seasons without percentiles, NULL = not looked up yet
    current_score       numeric,
    current_pct         numeric,
    current_tier        text,                   -- provisional while the season runs
    -- Blizzard API (account-wide values: max over characters)
    pvp_best_rating     int,                    -- current PvP season, any bracket
    pvp_best_bracket    text,
    honor_level         int,
    achievement_points  int,
    mounts              int,
    pets                int,
    toys                int,
    last_login          timestamptz,
    refreshed_at        timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE people ADD COLUMN IF NOT EXISTS best_milestone text;          -- highest keystone achievement any season
ALTER TABLE people ADD COLUMN IF NOT EXISTS best_milestone_season text;
ALTER TABLE people ADD COLUMN IF NOT EXISTS current_milestone text;

INSERT INTO sources (name) VALUES ('blizzard_forums'), ('youtube'), ('raiderio'), ('blizzard_api') ON CONFLICT DO NOTHING;
