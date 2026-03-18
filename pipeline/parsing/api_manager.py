"""Gemini API key manager with rotation and failure tracking."""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from google import genai

logger = logging.getLogger(__name__)


class APIKeyManager:
    """Manages multiple Gemini API keys with rotation on failure/rate-limit.

    Keys cycle on 429 (rate limit) or repeated failures.  When all keys are
    exhausted the manager signals the caller to stop.
    """

    def __init__(
        self,
        api_keys: List[Dict[str, str]],
        max_failures: int = 3,
        max_rate_limit_hits: int = 2,
    ) -> None:
        if not api_keys:
            raise ValueError("At least one API key is required")
        self.api_keys = api_keys
        self.clients = {k["id"]: genai.Client(api_key=k["key"]) for k in api_keys}
        self.stats: Dict[str, Dict[str, int]] = {
            k["id"]: {"fails": 0, "rate_limits": 0} for k in api_keys
        }
        self._idx = 0
        self._max_failures = max_failures
        self._max_rate_limits = max_rate_limit_hits

    # -- Properties ----------------------------------------------------------

    @property
    def current_id(self) -> str:
        return self.api_keys[self._idx]["id"]

    @property
    def current_client(self) -> genai.Client:
        return self.clients[self.current_id]

    # -- Rotation ------------------------------------------------------------

    def rotate(self) -> bool:
        """Switch to the next usable key.  Returns False if all exhausted."""
        start = self._idx
        while True:
            self._idx = (self._idx + 1) % len(self.api_keys)
            s = self.stats[self.current_id]
            if (
                s["fails"] < self._max_failures
                and s["rate_limits"] < self._max_rate_limits
            ):
                logger.info("Rotated to API key '%s'", self.current_id)
                return True
            if self._idx == start:
                return False

    def record_success(self) -> None:
        self.stats[self.current_id]["fails"] = 0

    def record_failure(self) -> bool:
        self.stats[self.current_id]["fails"] += 1
        return self.rotate()

    def record_rate_limit(self) -> bool:
        self.stats[self.current_id]["rate_limits"] += 1
        logger.warning("Rate limit on key '%s'", self.current_id)
        return self.rotate()

    def status(self) -> str:
        active = sum(
            1
            for k in self.api_keys
            if self.stats[k["id"]]["fails"] < self._max_failures
            and self.stats[k["id"]]["rate_limits"] < self._max_rate_limits
        )
        parts = [
            f"{kid}: {s['fails']}F/{s['rate_limits']}RL"
            for kid, s in self.stats.items()
        ]
        return f"Active: {active}/{len(self.api_keys)} [{' | '.join(parts)}]"
