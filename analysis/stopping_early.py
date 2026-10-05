"""Charts for the blog post "Why stopping early costs you rank" (aggregate data only).

    python analysis/stopping_early.py [output_dir]

Reads Raider.IO season cutoffs from the database (mplus_cutoff_history and
mplus_percentile_points; run `python -m epic_parse snapshot` or `fetch raiderio` first)
and writes PNG charts plus a CSV per chart (the table view) to output_dir
(default: docs/blog/images).

Data: Raider.IO (https://raider.io), US region.
"""
import csv
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter, MultipleLocator  # noqa: E402

from epic_parse.db import connect  # noqa: E402

OUT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent / "docs" / "blog" / "images"

# Reference palette (light mode), categorical slots in documented order; chart chrome tokens.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
SURFACE, INK, INK_2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
SOURCE = "Data: Raider.IO (raider.io), US region"

# Completed seasons with full cutoff history. DF S1/S2 are left out: Raider.IO reports
# far fewer players for them than for any other season, so their percentiles look unreliable.
SEASONS = {
    "season-sl-3": "Shadowlands S3", "season-sl-4": "Shadowlands S4",
    "season-df-3": "Dragonflight S3", "season-df-4": "Dragonflight S4",
    "season-tww-1": "TWW S1", "season-tww-2": "TWW S2", "season-tww-3": "TWW S3",
    "season-mn-1": "Midnight S1",
}
HIGHLIGHT = ["season-tww-1", "season-tww-2", "season-tww-3", "season-mn-1"]  # the four lines drawn


def style(ax, title: str, subtitle: str) -> None:
    fig = ax.figure
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(AXIS)
    ax.tick_params(colors=MUTED, labelsize=9, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    fig.text(0.06, 0.95, title, fontsize=13, fontweight="bold", color=INK, ha="left", va="top")
    fig.text(0.06, 0.895, subtitle, fontsize=9.5, color=INK_2, ha="left", va="top")
    fig.text(0.06, 0.02, SOURCE, fontsize=8, color=MUTED, ha="left", va="bottom")


def write_csv(name: str, header: list[str], rows: list) -> None:
    with open(OUT / f"{name}.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def chart_moving_line(conn) -> None:
    """How the top 0.1% / 1% / 10% lines climbed through Midnight Season 1."""
    rows = conn.execute(
        """SELECT h.percentile, (extract(epoch FROM h.at - ms.starts) / 604800)::float AS week, h.min_score::float
           FROM mplus_cutoff_history h JOIN mplus_seasons ms ON ms.slug = h.season
           WHERE h.season = 'season-mn-1' AND h.percentile IN (99.9, 99, 90) AND h.at >= ms.starts
           ORDER BY h.percentile DESC, h.at"""
    ).fetchall()
    fig, ax = plt.subplots(figsize=(8, 4.8), dpi=150)
    fig.subplots_adjust(left=0.08, right=0.84, top=0.80, bottom=0.14)
    for (pct, label), color in zip([(99.9, "Top 0.1%"), (99.0, "Top 1%"), (90.0, "Top 10%")], SERIES):
        pts = [(w, s) for p, w, s in rows if float(p) == pct]
        ax.plot([w for w, _ in pts], [s for _, s in pts], color=color, linewidth=2, label=label)
        ax.annotate(f"{label}  {pts[-1][1]:,.0f}", (pts[-1][0], pts[-1][1]), xytext=(6, 0), textcoords="offset points",
                    fontsize=9, color=INK_2, va="center")
    ax.set_xlabel("Weeks into the season", color=MUTED, fontsize=9)
    ax.xaxis.set_major_locator(MultipleLocator(4))
    ax.set_ylabel("Minimum score", color=MUTED, fontsize=9)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.legend(loc="lower right", frameon=False, fontsize=9, labelcolor=INK_2)
    style(ax, "The line keeps moving", "Score needed to be in the top 0.1%, 1% and 10% of players, Midnight Season 1")
    fig.savefig(OUT / "moving-line-mn1.png", facecolor=SURFACE)
    plt.close(fig)
    write_csv("moving-line-mn1", ["percentile", "week", "min_score"], [(p, round(w, 2), round(s)) for p, w, s in rows])


def chart_cost_of_stopping(conn) -> list:
    """If your score stops at the top-1% line in week N, where do you finish?"""
    data = {}
    for slug in SEASONS:
        data[slug] = conn.execute(
            """WITH weekly AS (
                   SELECT DISTINCT ON (wk) floor(extract(epoch FROM h.at - ms.starts) / 604800)::int AS wk, h.min_score
                   FROM mplus_cutoff_history h JOIN mplus_seasons ms ON ms.slug = h.season
                   WHERE h.season = %(s)s AND h.percentile = 99 AND h.at >= ms.starts
                   ORDER BY wk, h.at DESC)
               SELECT wk, min_score::float, mplus_percentile(%(s)s::text, min_score::numeric)::float
               FROM weekly WHERE wk >= 1 ORDER BY wk""",
            {"s": slug},
        ).fetchall()
    fig, ax = plt.subplots(figsize=(8, 4.8), dpi=150)
    fig.subplots_adjust(left=0.08, right=0.96, top=0.80, bottom=0.14)
    for slug, color in zip(HIGHLIGHT, SERIES):
        pts = [(w, p) for w, _, p in data[slug] if p is not None]
        ax.plot([w for w, _ in pts], [p for _, p in pts], color=color, linewidth=2, label=SEASONS[slug])
    # One callout instead of end labels (the lines converge at 1% and the labels collide).
    w6 = next(p for w, _, p in data["season-mn-1"] if w == 6)
    ax.plot([6], [w6], marker="o", markersize=8, color=SERIES[3], markeredgecolor=SURFACE, markeredgewidth=2)
    ax.annotate(f"Midnight S1: stop in week 6,\nfinish around top {w6:.1f}%", (6, w6), xytext=(24, 28),
                textcoords="offset points", fontsize=9, color=INK_2,
                arrowprops=dict(arrowstyle="-", color=MUTED, linewidth=0.8))
    ax.axhline(5, color=AXIS, linewidth=1)
    ax.text(0.6, 5.25, "top 5%", fontsize=8.5, color=MUTED)
    ax.axhline(1, color=AXIS, linewidth=1)
    ax.text(0.6, 1.25, "top 1% (where you stopped)", fontsize=8.5, color=MUTED)
    ax.set_ylim(0, 16)
    ax.set_xlabel("Week you stopped playing", color=MUTED, fontsize=9)
    ax.xaxis.set_major_locator(MultipleLocator(2))
    ax.set_ylabel("Finished the season in the top …%", color=MUTED, fontsize=9)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.legend(loc="upper right", frameon=False, fontsize=9, labelcolor=INK_2)
    style(ax, "What stopping early costs",
          "You're exactly at the top-1% line in week N and never play again. Where do you finish?")
    fig.savefig(OUT / "cost-of-stopping.png", facecolor=SURFACE)
    plt.close(fig)
    rows = [(SEASONS[s], w, round(score), round(p, 2) if p is not None else None) for s in SEASONS for w, score, p in data[s]]
    write_csv("cost-of-stopping", ["season", "week_stopped", "score_frozen_at", "final_top_percent"], rows)
    return rows


def chart_participation(conn) -> list:
    """Share of M+ players reaching Keystone Hero (2,500) and Legend (3,000) each season."""
    rows = conn.execute(
        """SELECT p.season,
                  100 * max(p.fraction) FILTER (WHERE p.point = 'keystoneHero') AS hero,
                  100 * max(p.fraction) FILTER (WHERE p.point = 'keystoneLegend') AS legend
           FROM mplus_percentile_points p JOIN mplus_seasons ms ON ms.slug = p.season
           WHERE p.season = ANY(%s) GROUP BY p.season, ms.starts ORDER BY ms.starts""",
        # Shadowlands is left out: S3's "Hero" sat at ~3,000 rather than 2,500 and S4 has no Hero point.
        ([sl for sl in SEASONS if not sl.startswith("season-sl")] + ["season-mn-2"],),
    ).fetchall()
    labels = [SEASONS.get(s, "Midnight S2*") for s, _, _ in rows]
    x = list(range(len(rows)))
    fig, ax = plt.subplots(figsize=(8, 4.8), dpi=150)
    fig.subplots_adjust(left=0.08, right=0.96, top=0.80, bottom=0.22)
    for i, (name, color) in enumerate([("Keystone Hero (2,500)", SERIES[0]), ("Keystone Legend (3,000)", SERIES[1])]):
        pts = [(xi, float(r[1 + i])) for xi, r in zip(x, rows) if r[1 + i] is not None]
        ax.plot([p[0] for p in pts], [p[1] for p in pts], color=color, linewidth=2, marker="o", markersize=5,
                markeredgecolor=SURFACE, markeredgewidth=2, label=name)
        ax.annotate(f"{pts[-2][1]:.0f}%", pts[-2], xytext=(0, 8), textcoords="offset points", fontsize=9,
                    color=INK_2, ha="center")
    ax.set_xticks(x, labels, rotation=30, ha="right")
    ax.set_ylim(0, 60)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.set_ylabel("Share of all M+ players", color=MUTED, fontsize=9)
    ax.legend(loc="upper left", frameon=False, fontsize=9, labelcolor=INK_2)
    style(ax, "More rewards, more climbers",
          "Share of players earning each score achievement. *Midnight S2 still in progress.")
    fig.savefig(OUT / "participation.png", facecolor=SURFACE)
    plt.close(fig)
    out = [(lbl, round(float(h), 1) if h is not None else None, round(float(l), 1) if l is not None else None)
           for lbl, (_, h, l) in zip(labels, rows)]
    write_csv("participation", ["season", "keystone_hero_percent", "keystone_legend_percent"], out)
    return out


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    plt.rcParams["font.family"] = ["Segoe UI", "DejaVu Sans"]
    with connect() as conn:
        chart_moving_line(conn)
        cost = chart_cost_of_stopping(conn)
        part = chart_participation(conn)
    print(f"Wrote charts and CSVs to {OUT}")
    for row in part:
        print("  participation", row)
