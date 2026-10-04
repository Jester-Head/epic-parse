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
    raw_page_id bigint REFERENCES raw_pages,
    extra       jsonb NOT NULL DEFAULT '{}',   -- likes, class, race, quotes, ...
    body_tsv    tsvector GENERATED ALWAYS AS (to_tsvector('english', coalesce(body, ''))) STORED,
    UNIQUE (source_id, source_key)
);
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

INSERT INTO sources (name) VALUES ('blizzard_forums'), ('youtube') ON CONFLICT DO NOTHING;
