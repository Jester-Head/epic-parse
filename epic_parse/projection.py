"""End-of-season projections of Mythic+ percentile lines (our own; Raider.IO's site projection
isn't in its public API).

Idea: every line climbs through a season in a broadly similar shape. For each finished season we
record, day by day, the line's value as a share of its final value (mplus_line_progress). A running
season's final is then projected as today's value / the median share other seasons had reached by
the same day (SQL: mplus_cutoff_projection). Ratios don't depend on score scale, so seasons with
key-level squishes or scoring changes still compare. Season length is the main unknown: short
seasons settle earlier, and a running season's end date is often not published yet.

refresh():  rebuild mplus_line_progress from the cutoff history (runs after each cutoff refresh).
backtest(): leave-one-out check on finished seasons: projection error by week of season.
report():   current projections for a season.
"""
import logging

log = logging.getLogger(__name__)

PERCENTILES = [99.9, 99.0, 95.0, 90.0, 75.0, 60.0]

# Seasons left out of the model because their history is unreliable.
EXCLUDED = {
    "season-df-1": "Raider.IO's archive counts only ~45k players; projections from it were 18-30% off at week 4",
}


def refresh(conn) -> int:
    """Rebuild the progress model for every finished season that has cutoff history."""
    with conn.transaction():
        conn.execute("DELETE FROM mplus_line_progress")
        n = conn.execute(
            """
            WITH seasons AS (
                SELECT s.slug, s.starts, s.ends - s.starts AS length
                FROM mplus_seasons s
                WHERE s.ends < now() AND s.slug <> ALL(%(excluded)s::text[])
                  AND EXISTS (SELECT 1 FROM mplus_cutoff_history h WHERE h.season = s.slug)
            ), daily AS (   -- each line's value as of the end of each day
                SELECT s.slug AS season, p.pct AS percentile, d.day,
                       mplus_score_at(s.slug, 100 - p.pct, (s.starts + d.day + 1)::timestamptz) AS value
                FROM seasons s
                CROSS JOIN unnest(%(pcts)s::numeric[]) AS p(pct)
                CROSS JOIN LATERAL generate_series(0, s.length) AS d(day)
            ), finals AS (
                SELECT DISTINCT ON (season, percentile) season, percentile, value AS final
                FROM daily WHERE value > 0 ORDER BY season, percentile, day DESC
            )
            INSERT INTO mplus_line_progress (season, percentile, day, value, final, share)
            SELECT d.season, d.percentile, d.day, d.value, f.final, round(d.value / f.final, 5)
            FROM daily d JOIN finals f USING (season, percentile)
            WHERE d.value > 0
            """,
            {"pcts": PERCENTILES, "excluded": list(EXCLUDED)},
        ).rowcount
    log.info("Projection model rebuilt: %s day-points from finished seasons", f"{n:,}")
    return n


def backtest(conn, weeks=(2, 4, 6, 8, 10, 12, 16)) -> list[tuple]:
    """For each finished season, project its final from the other seasons at week N and compare.

    Returns rows (percentile, week, seasons, median abs error %, worst abs error %, share of
    seasons whose actual final fell inside the low-high range).
    """
    rows = conn.execute(
        """
        WITH cases AS (
            SELECT lp.season, lp.percentile, w.week, lp.final, s.starts
            FROM unnest(%(weeks)s::int[]) AS w(week)
            JOIN mplus_line_progress lp ON lp.day = w.week * 7
            JOIN mplus_seasons s ON s.slug = lp.season
        ), proj AS (
            SELECT c.*, p.projected, p.low, p.high
            FROM cases c, LATERAL mplus_cutoff_projection(c.season, c.percentile,
                                                           (c.starts + c.week * 7 + 1)::timestamptz) p
            WHERE p.projected IS NOT NULL
        )
        SELECT percentile, week, count(*),
               round(percentile_cont(0.5) WITHIN GROUP (ORDER BY abs(projected - final) / final * 100)::numeric, 1),
               round(max(abs(projected - final) / final * 100), 1),
               round(100.0 * count(*) FILTER (WHERE final BETWEEN low AND high) / count(*))
        FROM proj GROUP BY 1, 2 ORDER BY 1 DESC, 2
        """,
        {"weeks": list(weeks)},
    ).fetchall()
    return rows


def report(conn, season: str | None = None) -> tuple[str, list[tuple]]:
    season = season or conn.execute(
        "SELECT slug FROM mplus_seasons WHERE starts <= now() ORDER BY starts DESC LIMIT 1").fetchone()[0]
    return season, conn.execute(
        """SELECT pct, p.day, p.current_score, p.projected, p.low, p.high, p.seasons_used
           FROM unnest(%(pcts)s::numeric[]) AS pct, LATERAL mplus_cutoff_projection(%(season)s, pct, now()) p
           ORDER BY pct DESC""",
        {"pcts": PERCENTILES, "season": season},
    ).fetchall()
