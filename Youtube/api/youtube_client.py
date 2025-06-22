# File: api/youtube_client.py
import json
import logging
import random
import time
from itertools import cycle
from typing import Tuple, Optional, Any, Callable

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from Youtube.config import API_KEYS

logger = logging.getLogger(__name__)


class QuotaExhaustedError(Exception):
    """Exception raised when a quota limit is exceeded.

    This exception is intended to be used in scenarios where a resource limit,
    such as an API call quota or database query limit, is exhausted. It provides
    a way to signal that an operation cannot proceed due to exceeding the
    pre-defined limits.
    """
    pass


class YouTubeClient:
    """
    Client to interact with the YouTube API.

    This class manages the interaction with the YouTube API, including key rotation for
    handling quota limits, exponential backoff for retries, and error handling for
    various API response scenarios. It creates a service instance and retries requests
    when necessary while supporting multiple API keys to reduce downtime due to
    exhausted quotas.

    Attributes:
        api_keys (list): List of API keys to be used for requests. If not provided,
            a default set of keys will be used.
        api_key_cycle (itertools.cycle): Iterator cycling through API keys to enable
            key rotation.
        global_backoff_time (int): Time in seconds to wait before retrying after all
            API keys have been exhausted.
        last_global_exhaust_time (float): Timestamp of the last occurrence of all API
            keys being exhausted.
        service: Instance of the YouTube API service built using the current API key.
    """

    def __init__(self, api_keys: list = None):
        self.api_keys = api_keys or API_KEYS
        self.api_key_cycle = cycle(self.api_keys)
        self.global_backoff_time = 600  # 10 minutes
        self.last_global_exhaust_time = 0
        self.service = self._build_service()

    def _build_service(self):
        """Creates a YouTube API service instance."""
        api_key = next(self.api_key_cycle)
        logger.info("Using API key %s", api_key)
        return build("youtube", "v3", developerKey=api_key, cache_discovery=False)

    def retry_request(
            self,
            request_func: Callable,
            retries: int = 5,
            backoff_factor: float = 0.2
    ) -> Tuple[Optional[dict], Any]:
        """
        Retries a YouTube API request with exponential backoff and key rotation.

        Args:
            request_func: Function that generates the API request
            retries: Number of retries to attempt
            backoff_factor: Factor for exponential backoff

        Returns:
            Tuple of (response_dict, service_instance)
        """
        rotations = 0
        total_keys = len(self.api_keys)

        for attempt in range(retries):
            if (
                    rotations >= total_keys
                    and time.time() - self.last_global_exhaust_time < self.global_backoff_time
            ):
                wait = self.global_backoff_time - (time.time() - self.last_global_exhaust_time)
                logger.warning("All keys exhausted – sleeping %.1f s …", wait)
                time.sleep(wait)
                rotations = 0

            try:
                resp = request_func(self.service).execute()
                return resp, self.service

            except HttpError as e:
                reason = self._extract_error_reason(e)

                if e.resp.status in (500, 502, 503, 504):
                    time.sleep(backoff_factor * 2 ** attempt)
                    continue

                if e.resp.status == 403 and reason in {
                    "quotaExceeded", "dailyLimitExceeded", "userRateLimitExceeded"
                }:
                    rotations += 1
                    if rotations >= total_keys:
                        self.last_global_exhaust_time = time.time()
                        raise QuotaExhaustedError("All API keys exhausted")
                    self.service = self._build_service()
                    time.sleep(backoff_factor * 2 ** attempt + random.random() * 0.1)
                    continue

                logger.error("HttpError %s (%s) – not retrying", e.resp.status, reason)
                return None, self.service

            except Exception as ex:
                logger.error("Unexpected exception: %s", ex)
                return None, self.service

        logger.error("All retries failed")
        return None, self.service

    @staticmethod
    def _extract_error_reason(error: HttpError) -> Optional[str]:
        """Extract error reason from HttpError."""
        try:
            return json.loads(error.content.decode())["error"]["errors"][0]["reason"]
        except Exception:
            return None
