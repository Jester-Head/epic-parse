"""Tag posts with the WoW game version, expansion and patch that were live when they were written.

game_version: 'retail', 'classic' or 'forever', from the thread's forum category.
expansion:    retail: the expansion of the patch below (pre-patch weeks count as the new
              expansion, since its systems are already live); classic: the Classic flavor
              named by the forum (e.g. 'Season of Discovery'), or for the renamed progression
              forum the Classic expansion live at the time (by launch date); else 'Classic'.
patch:        retail only: the latest patch released on or before the post date.

Tagging runs in SQL so it is the same for freshly parsed posts (blizzard_forums.parse)
and for backfills (`python -m epic_parse tag-patches`).

To add a patch: append it to RETAIL_PATCHES. A new expansion also needs EXPANSIONS.
"""
import logging

from epic_parse.db import bulk_mode, source_id

log = logging.getLogger(__name__)

# North American release dates, checked against each patch's page on warcraft.wiki.gg
# (Patch_X.Y.Z, "Release date"). Minor x.y.5/x.y.7 patches are listed from 8.1.5 on.
RETAIL_PATCHES = [
    ("3.3.0", "2009-12-08"),
    ("4.0.1", "2010-10-12"), ("4.1.0", "2011-04-26"), ("4.2.0", "2011-06-28"), ("4.3.0", "2011-11-29"),
    ("5.0.4", "2012-08-28"), ("5.1.0", "2012-11-27"), ("5.2.0", "2013-03-05"), ("5.3.0", "2013-05-21"),
    ("5.4.0", "2013-09-10"),
    ("6.0.2", "2014-10-14"), ("6.1.0", "2015-02-24"), ("6.2.0", "2015-06-23"),
    ("7.0.3", "2016-07-19"), ("7.1.0", "2016-10-25"), ("7.2.0", "2017-03-28"), ("7.3.0", "2017-08-29"),
    ("8.0.1", "2018-07-17"), ("8.1.0", "2018-12-11"), ("8.1.5", "2019-03-12"), ("8.2.0", "2019-06-25"),
    ("8.2.5", "2019-09-24"), ("8.3.0", "2020-01-14"), ("8.3.7", "2020-07-21"),
    ("9.0.1", "2020-10-13"), ("9.0.2", "2020-11-17"), ("9.1.0", "2021-06-29"), ("9.1.5", "2021-11-02"),
    ("9.2.0", "2022-02-22"), ("9.2.5", "2022-05-31"), ("9.2.7", "2022-08-16"),
    ("10.0.0", "2022-10-25"), ("10.0.2", "2022-11-15"), ("10.0.5", "2023-01-24"), ("10.0.7", "2023-03-21"),
    ("10.1.0", "2023-05-02"), ("10.1.5", "2023-07-11"), ("10.1.7", "2023-09-05"), ("10.2.0", "2023-11-07"),
    ("10.2.5", "2024-01-16"), ("10.2.6", "2024-03-19"), ("10.2.7", "2024-05-07"),
    ("11.0.0", "2024-07-23"), ("11.0.2", "2024-08-13"), ("11.0.5", "2024-10-22"), ("11.0.7", "2024-12-17"),
    ("11.1.0", "2025-02-25"), ("11.1.5", "2025-04-22"), ("11.1.7", "2025-06-17"), ("11.2.0", "2025-08-05"),
    ("11.2.5", "2025-10-07"), ("11.2.7", "2025-12-02"),
    ("12.0.0", "2026-01-20"), ("12.0.1", "2026-02-10"), ("12.0.5", "2026-04-21"), ("12.0.7", "2026-06-16"),
    ("12.1.0", "2026-08-11"),
]

EXPANSIONS = {
    3: "Wrath of the Lich King", 4: "Cataclysm", 5: "Mists of Pandaria", 6: "Warlords of Draenor",
    7: "Legion", 8: "Battle for Azeroth", 9: "Shadowlands", 10: "Dragonflight", 11: "The War Within",
    12: "Midnight",
}

# "Mists of Pandaria Classic Discussion" is one forum that Blizzard renamed with each
# Classic progression expansion (Burning Crusade -> Wrath -> Cataclysm -> Mists), so its
# posts are tagged by date instead of by name. Launch dates (US); posts before the first
# launch were pre-launch TBC Classic discussion.
PROGRESSION_FORUM = "mists of pandaria classic discussion"
CLASSIC_PROGRESSION = [
    ("2021-06-01", "The Burning Crusade Classic"),
    ("2022-09-26", "Wrath of the Lich King Classic"),
    ("2024-05-20", "Cataclysm Classic"),
    ("2025-07-21", "Mists of Pandaria Classic"),
]

# Other Classic forums: flavor by keyword in the forum name (checked in order, lowercase).
CLASSIC_FLAVORS = [
    ("season of discovery", "Season of Discovery"),
    ("hardcore", "Hardcore"),
    ("burning crusade", "The Burning Crusade Classic"),
    ("tbc", "The Burning Crusade Classic"),
    ("wrath", "Wrath of the Lich King Classic"),
    ("cataclysm", "Cataclysm Classic"),
    ("cata ", "Cataclysm Classic"),
    ("mists of pandaria", "Mists of Pandaria Classic"),
    ("mop", "Mists of Pandaria Classic"),
]


def tag_posts(conn, source: str = "blizzard_forums", overwrite: bool = False) -> int:
    """Tag posts of `source` that have no game_version yet (or all of them with overwrite).

    Returns the number of posts tagged. Uses bulk mode (search indexes rebuilt once)
    when touching more than BULK_THRESHOLD posts.
    """
    src = source_id(conn, source)
    with conn.transaction():
        conn.execute("CREATE TEMP TABLE wow_patches (patch text, released date, expansion text) ON COMMIT DROP")
        conn.execute("CREATE TEMP TABLE classic_flavors (pos int, keyword text, flavor text) ON COMMIT DROP")
        conn.execute("CREATE TEMP TABLE classic_progression (started date, flavor text) ON COMMIT DROP")
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO wow_patches VALUES (%s, %s, %s)",
                            [(p, d, EXPANSIONS[int(p.split(".")[0])]) for p, d in RETAIL_PATCHES])
            cur.executemany("INSERT INTO classic_flavors VALUES (%s, %s, %s)",
                            [(i, k, f) for i, (k, f) in enumerate(CLASSIC_FLAVORS)])
            cur.executemany("INSERT INTO classic_progression VALUES (%s, %s)", CLASSIC_PROGRESSION)
        todo = conn.execute(
            "SELECT count(*) FROM posts WHERE source_id = %s AND created_at IS NOT NULL"
            + ("" if overwrite else " AND NOT extra ? 'game_version'"),
            (src,),
        ).fetchone()[0]
        if not todo:
            return 0
        with bulk_mode(conn, enabled=todo > BULK_THRESHOLD):
            tagged = conn.execute(
                """
                WITH classified AS (
                    SELECT p.id, p.created_at,
                           lower(coalesce(t.category, '') || ' | ' || coalesce(t.extra->>'parent_category', '')) AS forum
                    FROM posts p JOIN threads t ON t.id = p.thread_id
                    WHERE p.source_id = %(src)s AND p.created_at IS NOT NULL
                      AND (%(overwrite)s OR NOT p.extra ? 'game_version')
                ), versioned AS (
                    SELECT id, created_at, forum,
                           CASE WHEN forum LIKE '%%forever%%' THEN 'forever'
                                WHEN forum ~ '(classic|season of discovery|hardcore)' THEN 'classic'
                                ELSE 'retail' END AS game_version
                    FROM classified
                )
                UPDATE posts p
                SET extra = (p.extra - 'game_version' - 'expansion' - 'patch') || jsonb_strip_nulls(jsonb_build_object(
                    'game_version', v.game_version,
                    'expansion', CASE v.game_version
                        WHEN 'retail' THEN r.expansion
                        WHEN 'classic' THEN CASE
                            WHEN v.forum LIKE %(progression)s || '%%' THEN coalesce(
                                (SELECT flavor FROM classic_progression
                                 WHERE started <= (v.created_at AT TIME ZONE 'UTC')::date ORDER BY started DESC LIMIT 1),
                                (SELECT flavor FROM classic_progression ORDER BY started LIMIT 1))
                            ELSE coalesce(
                                (SELECT flavor FROM classic_flavors WHERE v.forum LIKE '%%' || keyword || '%%' ORDER BY pos LIMIT 1),
                                'Classic')
                            END
                        END,
                    'patch', CASE WHEN v.game_version = 'retail' THEN r.patch END))
                FROM versioned v
                LEFT JOIN LATERAL (
                    SELECT patch, expansion FROM wow_patches
                    WHERE released <= (v.created_at AT TIME ZONE 'UTC')::date
                    ORDER BY released DESC LIMIT 1
                ) r ON true
                WHERE p.id = v.id
                """,
                {"src": src, "overwrite": overwrite, "progression": PROGRESSION_FORUM},
            ).rowcount
    log.info("Tagged %s %s posts with game version / expansion / patch", f"{tagged:,}", source)
    return tagged


BULK_THRESHOLD = 50_000
