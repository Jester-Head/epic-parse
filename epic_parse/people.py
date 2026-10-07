"""One row per person: forum accounts (and self-reported players) joined across their characters.

Raider.IO, the Blizzard API and the forums all describe *characters*; one person can have many
(alts). This rebuilds two tables from everything collected so far:

player_seasons  one row per person per Mythic+ season: their best retail character's score,
                percentile and tier (mplus_tier), plus how many of their characters had a score.
people          one row per person: forum activity, characters, Mythic+ history and current
                season, PvP and collections (account-wide values take the max over characters).

Rebuild after the collection jobs have added data: `python -m epic_parse people`.
Characters come from player_characters: posted as (forum_alias), listed on the forum profile
(account_alias) and self-reported. Classic characters are left out of the Mythic+ and Blizzard
columns (both cover retail only) but counted in `classic_characters`.
"""
import logging

log = logging.getLogger(__name__)

TIER_ORDER = ["casual", "mid1", "mid2", "hardcore"]  # 'none' and NULL rank below casual
MILESTONE_ORDER = ["Keystone Explorer", "Keystone Conqueror", "Keystone Master", "Keystone Hero",
                   "Keystone Legend", "Keystone Myth"]
# people.peak_tier also uses 'none' (looked up, no Mythic+ score), 'no_pct_data' (scores only in
# Legion-Shadowlands S2, which have no percentile data) and NULL (not looked up on Raider.IO yet).


def refresh(conn) -> tuple[int, int]:
    with conn.transaction():
        conn.execute("SET LOCAL work_mem = '256MB'")
        conn.execute("DELETE FROM player_seasons")
        seasons = conn.execute(
            """
            WITH cs AS (   -- every (person, retail character, season) with a score
                SELECT pc.player_id, ch.id AS character_id, ch.name || '-' || ch.realm AS character,
                       ch.class, e.key AS season, e.value::numeric AS score
                FROM player_characters pc
                JOIN characters ch ON ch.id = pc.character_id
                CROSS JOIN LATERAL jsonb_each_text(ch.mplus) e
                WHERE ch.classic IS NOT TRUE AND e.value ~ '^[0-9.]+$' AND e.value::numeric > 0
            ), best AS (
                SELECT DISTINCT ON (player_id, season) * FROM cs ORDER BY player_id, season, score DESC
            ), alts AS (
                SELECT player_id, season, count(DISTINCT character_id) AS n FROM cs GROUP BY 1, 2
            )
            INSERT INTO player_seasons (player_id, season, best_character_id, best_character, class, score,
                                        pct, tier, elite, characters_with_score, provisional, milestones, milestone)
            SELECT b.player_id, b.season, b.character_id, b.character, b.class, b.score,
                   mplus_percentile(b.season, b.score), mplus_tier(b.season, b.score),
                   mplus_percentile(b.season, b.score) <= 0.1, a.n, s.ends > now(), m.ms, m.ms[cardinality(m.ms)]
            FROM best b JOIN alts a USING (player_id, season) JOIN mplus_seasons s ON s.slug = b.season
            CROSS JOIN LATERAL (SELECT mplus_milestones(b.season, b.score) AS ms) m
            """
        ).rowcount

        conn.execute("DELETE FROM people")
        people = conn.execute(
            """
            WITH cur AS (
                SELECT slug FROM mplus_seasons WHERE starts <= now() AND ends > now() ORDER BY starts DESC LIMIT 1
            ), shared_ids AS (  -- characters linked to more than one account
                SELECT character_id FROM player_characters GROUP BY 1 HAVING count(DISTINCT player_id) > 1
            ), chars AS (
                SELECT pc.player_id, ch.*, si.character_id IS NOT NULL AS shared
                FROM player_characters pc JOIN characters ch ON ch.id = pc.character_id
                LEFT JOIN shared_ids si ON si.character_id = ch.id
            ), char_stats AS (
                SELECT player_id, count(*) AS characters,
                       count(*) FILTER (WHERE classic IS NOT TRUE) AS retail,
                       count(*) FILTER (WHERE classic) AS classic,
                       count(*) FILTER (WHERE found) AS found,
                       count(*) FILTER (WHERE shared) AS shared
                FROM chars GROUP BY 1
            ), fallback_main AS (   -- for people without Mythic+: highest level, then achievements
                SELECT DISTINCT ON (player_id) player_id, name || '-' || realm AS character, class
                FROM chars WHERE classic IS NOT TRUE
                ORDER BY player_id, level DESC NULLS LAST, achievement_points DESC NULLS LAST
            ), ps AS (
                SELECT ps.*, s.starts FROM player_seasons ps JOIN mplus_seasons s ON s.slug = ps.season
            ), mplus AS (
                SELECT player_id, count(*) AS seasons,
                       (array_agg(season ORDER BY starts))[1] AS first_season,
                       (array_agg(season ORDER BY starts DESC))[1] AS last_season,
                       (array_agg(best_character ORDER BY starts DESC))[1] AS main_character,
                       (array_agg(class ORDER BY starts DESC))[1] AS main_class,
                       min(pct) AS best_pct,
                       (array_agg(season ORDER BY pct ASC NULLS LAST))[1] AS best_pct_season,
                       (%(tiers)s::text[])[max(array_position(%(tiers)s::text[], tier))] AS peak_tier,
                       (%(milestones)s::text[])[max(array_position(%(milestones)s::text[], milestone))] AS best_milestone,
                       (array_agg(season ORDER BY array_position(%(milestones)s::text[], milestone) DESC NULLS LAST, starts DESC)
                           FILTER (WHERE milestone IS NOT NULL))[1] AS best_milestone_season
                FROM ps GROUP BY 1
            ), cur_season AS (
                SELECT ps.player_id, ps.score, ps.pct, ps.tier, ps.milestone FROM ps JOIN cur ON cur.slug = ps.season
            ), posts_by AS (   -- forum activity (crawled posts carry the account name as author)
                SELECT author, count(*) AS n, min(created_at) AS first_post, max(created_at) AS last_post,
                       avg((extra->>'game_version' = 'retail')::int) AS retail_share
                FROM posts
                WHERE source_id = (SELECT id FROM sources WHERE name = 'blizzard_forums') AND NOT extra ? 'v1_import'
                GROUP BY 1
            ), top_cat AS (
                SELECT DISTINCT ON (p.author) p.author, t.category
                FROM posts p JOIN threads t ON t.id = p.thread_id
                WHERE p.source_id = (SELECT id FROM sources WHERE name = 'blizzard_forums') AND NOT p.extra ? 'v1_import'
                GROUP BY p.author, t.category ORDER BY p.author, count(*) DESC
            ), pvp AS (     -- best rating in the latest PvP season seen
                SELECT DISTINCT ON (c.player_id) c.player_id, (e.value->>'rating')::int AS rating, e.key AS bracket
                FROM chars c JOIN bnet_characters b ON b.character_id = c.id
                CROSS JOIN LATERAL jsonb_each(b.pvp) e
                WHERE (e.value->>'season')::int = (SELECT max((v->>'season')::int)
                                                   FROM bnet_characters, jsonb_each(pvp) x(k, v))
                ORDER BY c.player_id, (e.value->>'rating')::int DESC NULLS LAST
            ), bnet AS (
                SELECT c.player_id, max(b.honor_level) AS honor_level, max(b.achievement_points) AS ap,
                       max(b.mounts) AS mounts, max(b.pets) AS pets, max(b.toys) AS toys,
                       max(b.last_login) AS last_login
                FROM chars c JOIN bnet_characters b ON b.character_id = c.id AND b.found
                GROUP BY 1
            )
            INSERT INTO people (player_id, key, forum_username, account_created, forum_post_count, posts_collected,
                                first_post, last_post, retail_post_share, top_category, characters, retail_characters,
                                classic_characters, raiderio_found, shared_characters, main_character, main_class,
                                mplus_seasons, first_mplus_season, last_mplus_season, best_pct, best_pct_season,
                                peak_tier, current_score, current_pct, current_tier, pvp_best_rating, pvp_best_bracket,
                                honor_level, achievement_points, mounts, pets, toys, last_login,
                                best_milestone, best_milestone_season, current_milestone)
            SELECT pl.id, pl.key, u.username, fa.account_created, fa.post_count, pb.n,
                   pb.first_post, pb.last_post, round(pb.retail_share, 3), tc.category,
                   coalesce(cs.characters, 0), coalesce(cs.retail, 0), coalesce(cs.classic, 0),
                   coalesce(cs.found, 0), coalesce(cs.shared, 0),
                   coalesce(m.main_character, fm.character), coalesce(m.main_class, fm.class),
                   coalesce(m.seasons, 0), m.first_season, m.last_season, m.best_pct, m.best_pct_season,
                   CASE WHEN m.peak_tier IS NOT NULL THEN m.peak_tier
                        WHEN coalesce(m.seasons, 0) > 0 THEN 'no_pct_data'  -- scores only in seasons without percentiles
                        WHEN cs.found > 0 THEN 'none' END,
                   cu.score, cu.pct, cu.tier, pv.rating, pv.bracket,
                   bn.honor_level, bn.ap, bn.mounts, bn.pets, bn.toys, bn.last_login,
                   m.best_milestone, m.best_milestone_season, cu.milestone
            FROM players pl
            LEFT JOIN LATERAL (SELECT CASE WHEN pl.key LIKE 'forum:%%' THEN substr(pl.key, 7) END AS username) u ON true
            LEFT JOIN forum_accounts fa ON fa.username = u.username
            LEFT JOIN posts_by pb ON pb.author = u.username
            LEFT JOIN top_cat tc ON tc.author = u.username
            LEFT JOIN char_stats cs ON cs.player_id = pl.id
            LEFT JOIN fallback_main fm ON fm.player_id = pl.id
            LEFT JOIN mplus m ON m.player_id = pl.id
            LEFT JOIN cur_season cu ON cu.player_id = pl.id
            LEFT JOIN pvp pv ON pv.player_id = pl.id
            LEFT JOIN bnet bn ON bn.player_id = pl.id
            """,
            {"tiers": TIER_ORDER, "milestones": MILESTONE_ORDER},
        ).rowcount
    log.info("Rebuilt people: %s people, %s person-seasons", f"{people:,}", f"{seasons:,}")
    return people, seasons
