"""A polite HTTP fetcher that stores every response in raw_pages."""
import logging
import time

import httpx
from psycopg.types.json import Jsonb

log = logging.getLogger(__name__)

USER_AGENT = "EpicParse/0.2 (WoW community research project; https://github.com/Jester-Head/epic-parse)"


class Fetcher:
    """Fetches JSON with a fixed delay between requests and retries on errors/rate limits.

    Every response is saved to raw_pages before it is returned, so parsing can be
    redone later without hitting the site again.
    """

    def __init__(self, conn, source_id: int, delay: float = 1.5, max_retries: int = 4):
        self.conn = conn
        self.source_id = source_id
        self.delay = delay
        self.max_retries = max_retries
        self.client = httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=30, follow_redirects=True)
        self._last_request = 0.0
        self.last_status: int | None = None
        self.last_headers: httpx.Headers = httpx.Headers()

    def _wait(self) -> None:
        elapsed = time.monotonic() - self._last_request
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self._last_request = time.monotonic()

    def get_json(self, url: str, kind: str, params=None, transform=None) -> tuple[int, dict | None]:
        """GET a JSON endpoint and save it. Returns (raw_page_id, parsed JSON or None).

        `transform`, if given, reduces a successful response before it is saved (and returned),
        for responses too big to keep whole. The HTTP status is left in `self.last_status`.
        """
        for attempt in range(1, self.max_retries + 1):
            self._wait()
            try:
                resp = self.client.get(url, params=params)
            except httpx.TransportError as e:
                log.warning("Network error on %s (attempt %d): %s", url, attempt, e)
                time.sleep(5 * attempt)
                continue
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = int(resp.headers.get("Retry-After", 10 * attempt))
                log.warning("HTTP %d on %s, waiting %ds (attempt %d)", resp.status_code, url, wait, attempt)
                time.sleep(wait)
                continue
            break
        else:
            raise RuntimeError(f"Giving up on {url} after {self.max_retries} attempts")

        try:
            data = resp.json()
        except ValueError:
            data = None
        if transform is not None and resp.status_code == 200 and data is not None:
            data = transform(data)
        self.last_status = resp.status_code
        self.last_headers = resp.headers
        with self.conn.transaction():
            raw_id = self.conn.execute(
                "INSERT INTO raw_pages (source_id, kind, url, status, body) VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (self.source_id, kind, str(resp.url), resp.status_code, Jsonb(data) if data is not None else None),
            ).fetchone()[0]
        if resp.status_code != 200:
            log.warning("HTTP %d on %s (saved as raw page %d)", resp.status_code, url, raw_id)
            return raw_id, None
        return raw_id, data

    def close(self) -> None:
        self.client.close()
