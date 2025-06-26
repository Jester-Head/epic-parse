# File: Youtube/api/youtube_client.py

import json
import logging
import random
import time
from itertools import cycle
from time import time
from typing import Tuple, Optional, Any, Callable

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config import API_KEYS

logger = logging.getLogger(__name__)


class QuotaExhaustedError(Exception):
    """
    Exception raised when a quota limit is exceeded.
    """
    pass


class YouTubeClient:
    """
    Client to interact with the YouTube API.

    This class manages API key rotation, handles quota exhaustion, and retries
    requests to the YouTube API with exponential backoff in case of transient errors.

    Attributes:
        api_keys (list): A list of API keys for interacting with the YouTube API.
        api_key_cycle (cycle): A cycle iterator for rotating through API keys.
        global_backoff_time (int): The time (in seconds) to wait when all API keys are exhausted.
        last_global_exhaust_time (float): The timestamp of the last global quota exhaustion.
        service: The YouTube API service instance.
    """

    def __init__(self, api_keys: list = None):
        """
        Initializes the YouTubeClient with a list of API keys.

        Args:
            api_keys (list, optional): A list of API keys. Defaults to the API_KEYS from the config.
        """
        self.api_keys = api_keys or API_KEYS
        self.api_key_cycle = cycle(self.api_keys)
        self.global_backoff_time = 600
        self.last_global_exhaust_time = 0
        self.service = self._build_service()

    def _build_service(self):
        """
        Builds the YouTube API service instance using the next API key.

        Returns:
            The YouTube API service instance.
        """
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
        Executes a YouTube API request with retries and exponential backoff.

        Args:
            request_func (Callable): A function that constructs the API request.
            retries (int): The maximum number of retry attempts. Defaults to 5.
            backoff_factor (float): The factor for exponential backoff. Defaults to 0.2.

        Returns:
            Tuple[Optional[dict], Any]: A tuple containing the API response (if successful)
            and the YouTube service instance.
        """
        rotations = 0
        total_keys = len(self.api_keys)

        for attempt in range(retries):
            if rotations >= total_keys and time() - self.last_global_exhaust_time < self.global_backoff_time:
                wait = self.global_backoff_time - (time() - self.last_global_exhaust_time)
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

                if e.resp.status == 403 and reason in {"quotaExceeded", "dailyLimitExceeded", "userRateLimitExceeded"}:
                    rotations += 1
                    if rotations >= total_keys:
                        self.last_global_exhaust_time = time()
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
        """
        Extracts the error reason from an HttpError.

        Args:
            error (HttpError): The HttpError instance.

        Returns:
            Optional[str]: The error reason, or None if it cannot be extracted.
        """
        try:
            return json.loads(error.content.decode())["error"]["errors"][0]["reason"]
        except Exception:
            return None