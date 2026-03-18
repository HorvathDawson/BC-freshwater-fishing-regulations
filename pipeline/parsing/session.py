"""Parsing session with checkpoint/resume support.

Tracks per-row parsing status and persists to disk after each batch.
On restart, already-successful rows are skipped automatically.
"""

from __future__ import annotations

import json
import logging
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .models import ParsedEntry

logger = logging.getLogger(__name__)

# Session state version — bump if schema changes
_SESSION_VERSION = 1


class ParsingSession:
    """Manages checkpoint/resume state for a parsing run.

    After each batch, results are persisted to ``session_state.json``
    inside the session directory.  On restart the session file is loaded
    and already-successful rows are skipped.

    Parameters
    ----------
    session_dir:
        Directory for session state + final output.
    total_rows:
        Number of rows to parse in this run.
    """

    def __init__(self, session_dir: Path, total_rows: int) -> None:
        self.session_dir = session_dir
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.session_file = session_dir / "session_state.json"

        self.total = total_rows
        self.results: List[Optional[Dict[str, Any]]] = [None] * total_rows
        self.status: List[str] = ["pending"] * total_rows
        self.started_at = datetime.now().isoformat()
        self.updated_at = self.started_at

        self._load_existing()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load_existing(self) -> None:
        """Load a previous session checkpoint if one exists."""
        if not self.session_file.exists():
            return
        try:
            with open(self.session_file, encoding="utf-8") as f:
                state = json.load(f)
            if state.get("version") != _SESSION_VERSION:
                logger.warning("Session version mismatch — starting fresh")
                return
            saved_total = state.get("total", 0)
            if saved_total != self.total:
                logger.warning(
                    "Row count changed (%d → %d) — starting fresh",
                    saved_total,
                    self.total,
                )
                return
            self.results = state["results"]
            self.status = state["status"]
            self.started_at = state.get("started_at", self.started_at)
            s = self.summary()
            logger.info(
                "Resumed session: %d success, %d failed, %d pending",
                s["success"],
                s["failed"],
                s["pending"],
            )
        except (json.JSONDecodeError, KeyError) as exc:
            logger.warning("Corrupt session file — starting fresh: %s", exc)

    def _save(self) -> None:
        """Atomically persist session state to disk."""
        self.updated_at = datetime.now().isoformat()
        state = {
            "version": _SESSION_VERSION,
            "total": self.total,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "results": self.results,
            "status": self.status,
        }
        # Atomic write: write to temp file then rename
        tmp_fd, tmp_path = tempfile.mkstemp(
            dir=self.session_dir, suffix=".tmp", prefix="session_"
        )
        try:
            with open(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False)
            Path(tmp_path).replace(self.session_file)
        except Exception:
            Path(tmp_path).unlink(missing_ok=True)
            raise

    # ------------------------------------------------------------------
    # Batch recording
    # ------------------------------------------------------------------

    def record_batch(
        self,
        start_idx: int,
        entries: List[Optional[ParsedEntry]],
    ) -> None:
        """Record results for a batch and save checkpoint.

        Parameters
        ----------
        start_idx:
            Global row index where this batch starts.
        entries:
            Parallel list of ParsedEntry (success) or None (failure).
        """
        for i, entry in enumerate(entries):
            idx = start_idx + i
            if idx >= self.total:
                break
            if entry is not None:
                self.results[idx] = entry.model_dump(mode="json")
                self.status[idx] = "success"
            else:
                self.status[idx] = "failed"
        self._save()

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def pending_indices(self) -> List[int]:
        """Return indices that still need parsing (pending or failed)."""
        return [i for i, s in enumerate(self.status) if s != "success"]

    def summary(self) -> Dict[str, int]:
        """Return counts by status."""
        return {
            "success": self.status.count("success"),
            "failed": self.status.count("failed"),
            "pending": self.status.count("pending"),
            "total": self.total,
        }

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------

    def revalidate(self, rows: List[Dict[str, Any]]) -> int:
        """Re-validate 'success' entries against current validation rules.

        Safe auto-corrections (e.g., tributary_only=true forcing
        includes_tributaries=true) are applied in-place without re-parsing.
        Entries that hard-fail current Pydantic validation are reset to
        'failed' for re-parsing.

        Returns count of entries reset (not counting silent fixes).
        """
        reset_count = 0
        fixed_count = 0
        for i in range(self.total):
            if self.status[i] != "success" or self.results[i] is None:
                continue

            stored = self.results[i]

            # Re-validate through current Pydantic model
            try:
                entry = ParsedEntry.model_validate(stored)
            except Exception as exc:
                water = (
                    rows[i].get("water", f"row_{i}") if i < len(rows) else f"row_{i}"
                )
                logger.info(
                    "Row %d (%s): resetting — model validation failed: %s",
                    i,
                    water,
                    exc,
                )
                self.status[i] = "failed"
                self.results[i] = None
                reset_count += 1
                continue

            # Apply safe auto-corrections in-place (no re-parse needed)
            validated = entry.model_dump(mode="json")
            patched = False
            for key in ("includes_tributaries", "tributary_only"):
                if stored.get(key) != validated.get(key):
                    stored[key] = validated[key]
                    patched = True

            if patched:
                water = (
                    rows[i].get("water", f"row_{i}") if i < len(rows) else f"row_{i}"
                )
                logger.info("Row %d (%s): auto-corrected in-place", i, water)
                fixed_count += 1

        if reset_count or fixed_count:
            self._save()
        if fixed_count:
            logger.info("  %d entries auto-corrected in-place", fixed_count)
        return reset_count

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------

    def finalize(self, output_path: Path) -> Path:
        """Write final output JSON containing only successful results.

        Returns the output path written.
        """
        parsed = [r for r in self.results if r is not None]
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(parsed, f, indent=2, ensure_ascii=False)
        return output_path
