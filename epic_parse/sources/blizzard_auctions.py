"""Auction house snapshots from Blizzard's API, summarized per item into auction_prices."""
import logging
from collections import defaultdict
from email.utils import parsedate_to_datetime
from urllib.parse import quote

from epic_parse.db import source_id
from epic_parse.fetch import Fetcher
from epic_parse.sources.blizzard_api import REGION, SOURCE, Token, _get, purge_expired

log = logging.getLogger(__name__)

RETAIL = f"dynamic-{REGION}"


def _connected_realm_id(href: str) -> int:
    return int(href.split("/connected-realm/")[1].split("?")[0].split("/")[0])


def summarize(auctions: list) -> list[tuple]:
    """(item_id, pet_species, auctions, quantity, min_price, median_price) per item, prices per unit.

    The median is weighted by quantity, so one stack of 200 counts as 200 units.
    """
    groups: dict[tuple[int, int], list] = defaultdict(lambda: [0, 0, []])
    for a in auctions:
        item, qty = a["item"], a.get("quantity") or 1
        g = groups[(item["id"], item.get("pet_species_id") or 0)]
        g[0] += 1
        g[1] += qty
        if "unit_price" in a:
            g[2].append((a["unit_price"], qty))
        elif a.get("buyout"):
            g[2].append((a["buyout"] // qty, qty))
    rows = []
    for (item_id, species), (count, qty, prices) in groups.items():
        low = median = None
        if prices:
            prices.sort()
            low, half, seen = prices[0][0], sum(q for _, q in prices) / 2, 0
            for price, q in prices:
                seen += q
                if seen >= half:
                    median = price
                    break
        rows.append((item_id, species, count, qty, low, median))
    return rows


def _snapshot(conn, fetcher: Fetcher, token: Token, namespace: str, pages: list[tuple[str, str]],
              connected_realm: int, house: int, label: str) -> int | None:
    """Fetch one auction house and store its summary. Returns the number of items, or None if not found.

    `pages` is the house's (path, kind) listings, summarized together as one snapshot.
    """
    auctions: list = []

    def reduce(data):
        page = data.get("auctions") or []
        auctions.extend(page)
        return {"auctions": len(page), "items": len(summarize(page))}

    modified = []
    for path, kind in pages:
        _, data, status = _get(fetcher, token, path, kind, transform=reduce,
                               params={"namespace": namespace, "locale": "en_US"})
        if status != 200 or data is None:
            return None
        if fetcher.last_headers.get("Last-Modified"):
            modified.append(parsedate_to_datetime(fetcher.last_headers["Last-Modified"]))
    rows = summarize(auctions)
    snapshot_at = max(modified) if modified else None
    key = (namespace, connected_realm, house, snapshot_at)
    if snapshot_at is None or conn.execute(
        """SELECT 1 FROM auction_prices WHERE namespace = %s AND connected_realm = %s AND house = %s
           AND snapshot_at = %s LIMIT 1""", key).fetchone():
        log.info("  %s %s: no new snapshot since %s", namespace, label, snapshot_at)
        return 0
    with conn.transaction(), conn.cursor() as cur, cur.copy(
        """COPY auction_prices (namespace, connected_realm, house, snapshot_at, item_id, pet_species,
                                auctions, quantity, min_price, median_price) FROM STDIN""") as copy:
        for row in rows:
            copy.write_row((*key, *row))
    log.info("  %s %s: %s auctions, %s items, snapshot %s", namespace, label, f"{len(auctions):,}",
             f"{len(rows):,}", snapshot_at)
    return len(rows)


def _realm_auctions(conn, fetcher: Fetcher, token: Token, namespace: str, cr: int) -> None:
    base = f"/data/wow/connected-realm/{cr}/auctions"
    if _snapshot(conn, fetcher, token, namespace, [(base, "bnet_auctions")], cr, 0, f"{cr} house 0") is not None:
        return
    # Classic Era style: a separate auction house per faction, listed in an index.
    params = {"namespace": namespace, "locale": "en_US"}
    _, index, status = _get(fetcher, token, base + "/index", "bnet_auction_index", params=params)
    houses = (index or {}).get("auctions") or []
    if status != 200 or not houses:
        log.warning("  %s %s: no auction house found", namespace, cr)
    for h in houses:
        if _snapshot(conn, fetcher, token, namespace, [(f"{base}/{h['id']}", "bnet_auctions")], cr, h["id"],
                     f"{cr} house {h['id']}") is None:
            log.warning("  %s %s house %s: not found", namespace, cr, h["id"])


def _ruleset_auctions(conn, fetcher: Fetcher, token: Token, namespace: str) -> None:
    params = {"namespace": namespace, "locale": "en_US"}
    _, index, _ = _get(fetcher, token, "/data/wow/ruleset/index", "bnet_ruleset_index", params=params)
    rulesets = sorted(r["id"] for r in (index or {}).get("rulesets", []))
    if not rulesets:
        log.warning("  %s: no rulesets found", namespace)
        return
    # Every ruleset lists the same houses with the same auctions, so one is enough.
    # The index only answers with the trailing slash.
    base = f"/data/wow/ruleset/{rulesets[0]}/auctions/"
    _, index, _ = _get(fetcher, token, base, "bnet_auction_index", params=params)
    houses = (index or {}).get("auctions") or []
    log.info("Auction houses in %s: %s", namespace, len(houses))
    if not houses:
        log.warning("  %s: no auction house found", namespace)
    for h in houses:
        pages = [(f"{base}{h['id']}", "bnet_auctions"), (f"{base}{h['id']}/commodities", "bnet_commodities")]
        try:
            if _snapshot(conn, fetcher, token, namespace, pages, 0, h["id"], f"house {h['id']}") is None:
                log.warning("  %s house %s: not found", namespace, h["id"])
        except RuntimeError as e:
            log.warning("  %s house %s skipped: %s", namespace, h["id"], e)


def resolve_realms(fetcher: Fetcher, token: Token, namespace: str, slugs: list[str] | None,
                   all_realms: bool) -> list[int]:
    params = {"namespace": namespace, "locale": "en_US"}
    if all_realms:
        _, index, _ = _get(fetcher, token, "/data/wow/connected-realm/index", "bnet_realm_index", params=params)
        return sorted(_connected_realm_id(r["href"]) for r in (index or {}).get("connected_realms", []))
    ids = []
    for slug in slugs or []:
        _, realm, _ = _get(fetcher, token, f"/data/wow/realm/{quote(slug)}", "bnet_realm", params=params)
        if realm and realm.get("connected_realm"):
            ids.append(_connected_realm_id(realm["connected_realm"]["href"]))
        else:
            log.warning("Realm %r not found in %s", slug, namespace)
    return sorted(set(ids))


def fetch(conn, namespace: str = RETAIL, realms: list[str] | None = None, all_realms: bool = False,
          commodities: bool = True, delay: float = 0.2) -> None:
    """Snapshot the region's commodities and the given realms' auction houses.

    Forever namespaces have no realms; every auction house there is snapshotted instead.
    Blizzard updates each auction house about once an hour; a snapshot already stored is skipped.
    """
    token = Token()
    fetcher = Fetcher(conn, source_id(conn, SOURCE), delay=delay)
    try:
        if "-forever-" in namespace:
            _ruleset_auctions(conn, fetcher, token, namespace)
        else:
            ids = resolve_realms(fetcher, token, namespace, realms, all_realms)
            log.info("Auction houses in %s: %s connected realm(s)%s", namespace, len(ids),
                     " and commodities" if commodities else "")
            if commodities and _snapshot(conn, fetcher, token, namespace,
                                         [("/data/wow/auctions/commodities", "bnet_commodities")], 0, 0,
                                         "commodities") is None:
                log.warning("  %s: no commodities auction house", namespace)
            for cr in ids:
                try:
                    _realm_auctions(conn, fetcher, token, namespace, cr)
                except RuntimeError as e:
                    log.warning("  %s %s skipped: %s", namespace, cr, e)
    finally:
        fetcher.close()
    purge_expired(conn)
