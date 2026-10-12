-- Epic-Parse schema. Safe to re-run: everything is CREATE ... IF NOT EXISTS.

CREATE TABLE IF NOT EXISTS sources (
    id   serial PRIMARY KEY,
    name text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS raw_pages (
    id         bigserial PRIMARY KEY,
    source_id  int NOT NULL REFERENCES sources,
    kind       text NOT NULL,
    url        text NOT NULL,
    status     int NOT NULL,
    body       jsonb,
    fetched_at timestamptz NOT NULL DEFAULT now(),
    parsed_at  timestamptz
);
CREATE INDEX IF NOT EXISTS raw_pages_unparsed ON raw_pages (source_id, kind) WHERE parsed_at IS NULL;
CREATE INDEX IF NOT EXISTS raw_pages_url ON raw_pages (url, fetched_at DESC);
CREATE INDEX IF NOT EXISTS raw_pages_topic_key ON raw_pages ((body->>'id'), (body->>'last_posted_at')) WHERE kind = 'topic';

CREATE TABLE IF NOT EXISTS threads (
    id             bigserial PRIMARY KEY,
    source_id      int NOT NULL REFERENCES sources,
    source_key     text NOT NULL,
    category       text,
    title          text,
    author         text,
    url            text,
    created_at     timestamptz,
    last_posted_at timestamptz,
    post_count     int,
    extra          jsonb NOT NULL DEFAULT '{}',
    first_seen     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_id, source_key)
);
CREATE INDEX IF NOT EXISTS threads_category ON threads (category);

CREATE TABLE IF NOT EXISTS posts (
    id          bigserial PRIMARY KEY,
    source_id   int NOT NULL REFERENCES sources,
    source_key  text NOT NULL,
    thread_id   bigint REFERENCES threads,
    parent_id   bigint REFERENCES posts,
    position    int,
    author      text,
    body        text,
    created_at  timestamptz,
    updated_at  timestamptz,
    raw_page_id bigint,                    -- the raw_pages row this came from (not a foreign key: see below)
    extra       jsonb NOT NULL DEFAULT '{}',
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

CREATE OR REPLACE FUNCTION realm_slug(text) RETURNS text
    LANGUAGE sql IMMUTABLE AS $$ SELECT lower(replace(replace(trim($1), '''', ''), ' ', '-')) $$;

CREATE TABLE IF NOT EXISTS mplus_seasons (
    slug         text PRIMARY KEY,
    name         text,
    expansion_id int,
    starts       date,
    ends         date
);

CREATE TABLE IF NOT EXISTS mplus_cutoffs (
    season      text REFERENCES mplus_seasons,
    percentile  numeric,
    min_score   numeric,
    population  int,
    fetched_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, percentile)
);

CREATE TABLE IF NOT EXISTS characters (
    id           bigserial PRIMARY KEY,
    region       text NOT NULL DEFAULT 'us',
    realm        text NOT NULL,
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

CREATE TABLE IF NOT EXISTS mplus_percentile_points (
    season     text REFERENCES mplus_seasons,
    point      text,                      -- 'p990', 'allTimed20', 'keystoneLegend', ...
    min_score  numeric,
    fraction   numeric,                   -- share of the season's players at or above min_score
    population int,
    PRIMARY KEY (season, point)
);

CREATE TABLE IF NOT EXISTS mplus_cutoff_history (
    season     text REFERENCES mplus_seasons,
    percentile numeric,
    at         timestamptz,
    min_score  numeric,
    players    int,
    PRIMARY KEY (season, percentile, at)
);

CREATE TABLE IF NOT EXISTS character_snapshots (
    character_id bigint REFERENCES characters,
    taken_at     timestamptz NOT NULL DEFAULT now(),
    season       text,
    score        numeric,
    item_level   numeric,
    runs         int,
    spec         text,
    raw_page_id  bigint,
    PRIMARY KEY (character_id, taken_at)
);

CREATE TABLE IF NOT EXISTS season_events (
    id      serial PRIMARY KEY,
    season  text,
    name    text NOT NULL,
    starts  date,
    ends    date,
    effect  text,
    source  text
);

-- Share of a season's players (in %) at or above p_score, e.g. 1.8 = top 1.8%. With p_at, as of that date.
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

-- The reverse of mplus_percentile: the score needed to be in the top p_pct percent.
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
    lo AS (SELECT s, f FROM pts WHERE round(f, 5) >= round((SELECT f FROM t), 5) ORDER BY f ASC, s DESC LIMIT 1),
    hi AS (SELECT s, f FROM pts WHERE round(f, 5) <= round((SELECT f FROM t), 5) ORDER BY f DESC, s ASC LIMIT 1)
    SELECT round(CASE
        WHEN NOT EXISTS (SELECT 1 FROM lo) OR NOT EXISTS (SELECT 1 FROM hi) THEN NULL
        WHEN (SELECT f FROM hi) = (SELECT f FROM lo) THEN (SELECT s FROM lo)
        ELSE (SELECT s FROM lo) + (ln((SELECT f FROM t)) - ln((SELECT f FROM lo)))
             / (ln((SELECT f FROM hi)) - ln((SELECT f FROM lo))) * ((SELECT s FROM hi) - (SELECT s FROM lo))
    END, 2)
$$;

CREATE TABLE IF NOT EXISTS mplus_line_progress (
    season     text,
    percentile numeric,
    day        int,
    value      numeric,
    final      numeric,
    share      numeric,
    PRIMARY KEY (season, percentile, day)
);

-- Projected end-of-season value of a percentile line: its value on p_at divided by the median
-- share of their final value that other finished seasons had reached by that day.
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

ALTER TABLE mplus_cutoffs ADD COLUMN IF NOT EXISTS derived boolean NOT NULL DEFAULT false;

-- false = Raider.IO has no cutoffs for the season, so it isn't requested again.
ALTER TABLE mplus_seasons ADD COLUMN IF NOT EXISTS has_cutoffs boolean;

CREATE TABLE IF NOT EXISTS unknown_realms (
    region     text NOT NULL,
    realm      text NOT NULL,
    first_seen timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (region, realm)
);

CREATE TABLE IF NOT EXISTS season_rules (
    season          text PRIMARY KEY,
    rating_system   text,
    affix_split     boolean,
    ksm_requirement text,
    key_squish      text,
    notes           text,
    sources         text
);
INSERT INTO season_rules (season, rating_system, affix_split, ksm_requirement, key_squish, sources) VALUES
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
    role         text,
    PRIMARY KEY (player_id, character_id)
);
CREATE INDEX IF NOT EXISTS player_characters_character ON player_characters (character_id);

-- season NULL = applies to the player in general.
CREATE TABLE IF NOT EXISTS gold_labels (
    id         bigserial PRIMARY KEY,
    player_id  bigint REFERENCES players,
    season     text,
    trait      text NOT NULL,
    value      text NOT NULL,
    confidence text NOT NULL DEFAULT 'confirmed',   -- 'confirmed', 'boundary', 'needs_review', or
                                                    -- 'self_reported' (the player's own answer, not verified)
    note       text,
    labeled_by text NOT NULL,
    labeled_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS forum_accounts (
    username        text PRIMARY KEY,
    found           boolean,                  -- false = profile gone (404)
    user_id         bigint,
    display_name    text,
    bio             text,
    account_created timestamptz,
    last_posted_at  timestamptz,
    last_seen_at    timestamptz,
    post_count      int,
    time_read_s     bigint,
    profile_views   int,
    alias_count     int,
    fetched_at      timestamptz,
    raw_page_id     bigint
);
ALTER TABLE forum_accounts ADD COLUMN IF NOT EXISTS failures int NOT NULL DEFAULT 0;

ALTER TABLE characters ADD COLUMN IF NOT EXISTS classic boolean;
-- Lookups that failed after all retries are skipped unless --retry-failed. Any real answer resets the count.
ALTER TABLE characters ADD COLUMN IF NOT EXISTS raiderio_failures int NOT NULL DEFAULT 0;
ALTER TABLE characters ADD COLUMN IF NOT EXISTS bnet_failures int NOT NULL DEFAULT 0;
ALTER TABLE characters ADD COLUMN IF NOT EXISTS level int;
ALTER TABLE characters ADD COLUMN IF NOT EXISTS achievement_points int;

CREATE TABLE IF NOT EXISTS bnet_characters (
    character_id           bigint PRIMARY KEY REFERENCES characters,
    found                  boolean,           -- false = 404: long inactive, very low level, or renamed/moved
    level                  int,
    last_login             timestamptz,
    item_level             int,
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
    raw_page_id            bigint
);
-- Must match blizzard_api.CHARACTER_KEY.
CREATE INDEX IF NOT EXISTS raw_pages_bnet_character ON raw_pages
    (((regexp_match(url, '/profile/wow/character/[^/?]+/[^/?]+'))[1])) WHERE kind LIKE 'bnet%';

CREATE OR REPLACE VIEW snapshot_pace AS
SELECT s.character_id, ch.name, ch.realm, s.season, s.taken_at, s.score, s.item_level,
       mplus_percentile(s.season, s.score, s.taken_at)            AS pct_same_day,
       (SELECT projected FROM mplus_cutoff_projection(s.season, 99.9, s.taken_at)) AS proj_top_0_1,
       (SELECT projected FROM mplus_cutoff_projection(s.season, 99,   s.taken_at)) AS proj_top_1,
       (SELECT projected FROM mplus_cutoff_projection(s.season, 95,   s.taken_at)) AS proj_top_5,
       (SELECT projected FROM mplus_cutoff_projection(s.season, 90,   s.taken_at)) AS proj_top_10
FROM character_snapshots s JOIN characters ch ON ch.id = s.character_id;

CREATE OR REPLACE FUNCTION mplus_tier_for_pct(p_score numeric, p_pct numeric) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN coalesce(p_score, 0) <= 0 THEN 'none'
        WHEN p_pct IS NULL THEN NULL
        WHEN p_pct <= 1 THEN 'hardcore'
        WHEN p_pct <= 5 THEN 'mid2'
        WHEN p_pct <= 20 THEN 'mid1'
        ELSE 'casual'
    END
$$;
CREATE OR REPLACE FUNCTION mplus_tier(p_season text, p_score numeric) RETURNS text
LANGUAGE sql STABLE AS $$
    SELECT mplus_tier_for_pct(p_score, mplus_percentile(p_season, p_score))
$$;

-- Keystone achievements a score qualifies for, lowest first. NULL = no threshold data for the season.
CREATE OR REPLACE FUNCTION mplus_milestones(p_season text, p_score numeric) RETURNS text[]
LANGUAGE sql STABLE AS $$
    SELECT CASE
        WHEN NOT EXISTS (SELECT 1 FROM mplus_percentile_points WHERE season = p_season AND point LIKE 'keystone%') THEN NULL
        ELSE coalesce((SELECT array_agg(replace(point, 'keystone', 'Keystone ') ORDER BY min_score)
                       FROM mplus_percentile_points
                       WHERE season = p_season AND point LIKE 'keystone%' AND p_score >= min_score), '{}')
    END
$$;

CREATE TABLE IF NOT EXISTS player_seasons (
    player_id             bigint,
    season                text,
    best_character_id     bigint,
    best_character        text,
    class                 text,
    score                 numeric,
    pct                   numeric,
    tier                  text,
    elite                 boolean,
    characters_with_score int,
    provisional           boolean,
    PRIMARY KEY (player_id, season)
);
ALTER TABLE player_seasons ADD COLUMN IF NOT EXISTS milestones text[];
ALTER TABLE player_seasons ADD COLUMN IF NOT EXISTS milestone text;

CREATE TABLE IF NOT EXISTS people (
    player_id           bigint PRIMARY KEY,
    key                 text,
    forum_username      text,
    account_created     timestamptz,
    forum_post_count    int,
    posts_collected     int,
    first_post          timestamptz,
    last_post           timestamptz,
    retail_post_share   numeric,
    top_category        text,
    characters          int,
    retail_characters   int,
    classic_characters  int,
    raiderio_found      int,
    shared_characters   int,
    main_character      text,
    main_class          text,
    mplus_seasons       int,
    first_mplus_season  text,
    last_mplus_season   text,
    best_pct            numeric,
    best_pct_season     text,
    peak_tier           text,                   -- best tier any season; 'none' = no M+ score, 'no_pct_data' = only
                                                -- scores in seasons without percentiles, NULL = not looked up yet
    current_score       numeric,
    current_pct         numeric,
    current_tier        text,
    pvp_best_rating     int,
    pvp_best_bracket    text,
    honor_level         int,
    achievement_points  int,
    mounts              int,
    pets                int,
    toys                int,
    last_login          timestamptz,
    refreshed_at        timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE people ADD COLUMN IF NOT EXISTS best_milestone text;
ALTER TABLE people ADD COLUMN IF NOT EXISTS best_milestone_season text;
ALTER TABLE people ADD COLUMN IF NOT EXISTS current_milestone text;

CREATE TABLE IF NOT EXISTS milestone_achievements (
    achievement_id int PRIMARY KEY,
    name           text,
    milestone      text,
    season         text
);

-- Blizzard achievements are account-wide, so a milestone may have been earned on another character.
CREATE TABLE IF NOT EXISTS character_milestones (
    character_id   bigint,
    achievement_id int,
    milestone      text,
    season         text,
    achieved_at    timestamptz,
    PRIMARY KEY (character_id, achievement_id)
);
CREATE INDEX IF NOT EXISTS character_milestones_season ON character_milestones (season, milestone);
-- Blizzard's API terms allow keeping its data for at most 30 days (blizzard_api.purge_expired).
ALTER TABLE character_milestones ADD COLUMN IF NOT EXISTS fetched_at timestamptz;
UPDATE character_milestones m SET fetched_at = b.fetched_at FROM bnet_characters b
WHERE m.fetched_at IS NULL AND b.character_id = m.character_id;
ALTER TABLE character_milestones ALTER COLUMN fetched_at SET DEFAULT now();

CREATE TABLE IF NOT EXISTS character_best_runs (
    character_id    bigint,
    keystone_run_id bigint,
    season          text,
    dungeon         text,
    mythic_level    int,
    score           numeric,
    completed_at    timestamptz,
    clear_time_ms   bigint,
    par_time_ms     bigint,
    upgrades        int,                  -- keystone levels gained (0 = depleted)
    spec            text,
    role            text,
    first_seen      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (character_id, keystone_run_id)
);
CREATE INDEX IF NOT EXISTS character_best_runs_season ON character_best_runs (season, completed_at);

ALTER TABLE player_seasons ADD COLUMN IF NOT EXISTS milestone_dates jsonb;   -- {"Keystone Master": "2026-09-02T...", ...}
ALTER TABLE player_seasons ADD COLUMN IF NOT EXISTS last_best_run_at timestamptz;

INSERT INTO sources (name) VALUES ('blizzard_forums'), ('youtube'), ('raiderio'), ('blizzard_api') ON CONFLICT DO NOTHING;
