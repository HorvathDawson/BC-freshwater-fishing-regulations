"""WRITE-THROUGH (T325, owner 2026-10-30): a saved mark reaches its final curated file by itself.

    "I don't want to have to individually lock everything. It should just work naturally from marks, and when
     reviewing items it should take my reviews and add to the curated items."

After the server appends a mark (one, or a batch: one request), it runs THAT DOMAIN's apply — the same
`pipeline.regs.review.apply.run` the command line runs: the marks' decisions (in memory), then the domain's writer.

  domain               writer                                   when
  sloughs, names,      file writer (slough_calls / mined_names / IN THE REQUEST (< 1 s) — the response says done
  tidal                tidal_limits, T326)
  dfo, entries,        apply_proposals: binding check, one       IN THE BACKGROUND, debounced (`DEBOUNCE_S`): the
  splits               commit per proposal (`git commit --`)     binding check is seconds warm, ~20 s cold. The
                                                                 response says "saving"; GET /api/review/write-through
                                                                 says when it is done and what became of each mark
  ponds                — closed (T329: nothing reads a pond call)

A file domain also goes to the background when an apply is already running (marking never waits on another apply).

NEVER LOSE A MARK: the mark is appended BEFORE any of this, and nothing here touches marks.jsonl. An apply that
refuses (a failed binding check, a file with uncommitted changes, a stale slough mark, conflicting decisions, any
error) leaves the mark in the store; the reason is returned (in-request) or held per item (background), shown on the
item in the app, and logged (`log_path`, one JSON line per run with a refusal). Every curated write is a temp file +
rename (`parsing.io.atomic_write`); apply_proposals puts a file back when its commit fails.

ON BY DEFAULT only when the server writes the curated marks store; a server over a temp store (every test, a second
copy of the tool) writes no curated file unless `CURATION_WRITE_THROUGH=1`. `CURATION_WRITE_THROUGH=0` turns it off.
"""
from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Callable, Iterable, Optional

from pipeline.regs.review import apply as AP

#: the domains whose writer is fast and file-only (run in the request)
FILE_DOMAINS = frozenset(AP.WRITERS)
#: seconds of quiet before a background apply starts (several quick marks -> one apply)
DEBOUNCE_S = float(os.environ.get("CURATION_WRITE_THROUGH_DEBOUNCE", "1.5"))
_log = logging.getLogger("uvicorn.error")


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def key(scope: str, target: str, rule_id: Optional[str] = None) -> str:
    return f"{scope}|{target}|{rule_id or ''}"


class WriteThrough:
    def __init__(self, marks: Path, *, enabled: bool,
                 log_path: Optional[Path] = None, debounce: float = DEBOUNCE_S,
                 runner: Optional[Callable[..., dict]] = None):
        self.marks, self.enabled = Path(marks), enabled
        self.log_path, self.debounce = log_path, debounce
        self.runner = runner
        self._apply = threading.Lock()            # one apply at a time: two writers never race on one curated file
        self._cv = threading.Condition()
        self._pending: dict[str, list[str]] = {}  # domain -> keys of the marks waiting for it
        self._last = 0.0                          # when the last background request came in
        self._busy = False
        self._worker: Optional[threading.Thread] = None
        self.domains: dict[str, dict] = {}        # domain -> {state, at, why?, files?}
        self.outcomes: dict[str, dict] = {}       # mark key -> {state, why, domain, at}

    # ------------------------------------------------------------------ entry point (after a mark is appended)

    def after_mark(self, scope: str, targets: Iterable[str], rule_id: Optional[str] = None) -> dict:
        """Called once per request, after the mark(s) are in the store. Returns what the response says."""
        dom = AP.domain_of(scope)
        if not self.enabled or dom is None:
            return {"state": "off" if not self.enabled else "none", "domain": dom}
        keys = [key(scope, t, rule_id) for t in targets]
        if dom in FILE_DOMAINS and self._apply.acquire(blocking=False):
            try:
                self._run([dom], {dom: keys})
            finally:
                self._apply.release()
            d = self.domains[dom]
            return {"domain": dom, "state": d["state"], "why": d.get("why"), "files": d.get("files", []),
                    "outcomes": {k: self.outcomes[k] for k in keys if k in self.outcomes}}
        self._schedule(dom, keys)
        return {"domain": dom, "state": "saving"}

    # ------------------------------------------------------------------ the background (debounced) runs

    def _schedule(self, dom: str, keys: list[str]) -> None:
        with self._cv:
            self._pending.setdefault(dom, []).extend(keys)
            self._last = time.monotonic()
            for k in keys:
                self.outcomes[k] = {"state": "saving", "domain": dom, "at": _now()}
            self.domains[dom] = {**self.domains.get(dom, {}), "state": "saving", "at": _now()}
            if self._worker is None or not self._worker.is_alive():
                self._worker = threading.Thread(target=self._loop, name="write-through", daemon=True)
                self._worker.start()
            self._cv.notify_all()

    def _loop(self) -> None:
        while True:
            with self._cv:
                if not self._pending:
                    self._worker = None
                    self._cv.notify_all()
                    return
                wait = self._last + self.debounce - time.monotonic()
                if wait > 0:
                    self._cv.wait(wait)
                    continue
                todo, self._pending = self._pending, {}
                self._busy = True
            try:
                with self._apply:
                    self._run(sorted(todo), todo)
            except Exception:                                      # noqa: BLE001 — the loop must not die
                _log.exception("write-through: background apply failed")
            finally:
                with self._cv:
                    self._busy = False
                    self._cv.notify_all()

    def wait_idle(self, timeout: float = 60.0) -> bool:
        """Block until no background apply is pending or running (tests; a clean shutdown)."""
        end = time.monotonic() + timeout
        with self._cv:
            while self._pending or self._busy or (self._worker is not None and self._worker.is_alive()):
                left = end - time.monotonic()
                if left <= 0:
                    return False
                self._cv.wait(min(left, 0.05))
        return True

    # ------------------------------------------------------------------ one apply

    def _run(self, domains: list[str], marked: dict[str, list[str]]) -> None:
        run = self.runner or AP.run
        t0 = time.monotonic()
        try:
            got = run(domains, marks=self.marks)
            err = None
        except Exception as ex:                                    # noqa: BLE001 — a refusal, said on the item
            _log.exception("write-through: apply %s failed", domains)
            got, err = {"rc": 1, "events": [], "files": []}, f"{type(ex).__name__}: {ex}"
        took = round(time.monotonic() - t0, 2)
        events = got.get("events") or []
        whole = err or next((e["why"] for e in events if e.get("status") == "refused" and not e.get("marks")), None)
        at = _now()
        # what became of each mark the run names (older marks too: a proposal still waiting on its side rebuild)
        for e in events:
            st = {"applied": "saved", "written": "saved", "would": "saved"}.get(e.get("status"), e.get("status"))
            for m in e.get("marks") or ():
                k = key(m.get("scope"), m.get("target"), m.get("rule_id"))
                self.outcomes[k] = {"state": st, "why": e.get("why"), "domain": e.get("domain"), "at": at,
                                    **({"sha": e["sha"]} if e.get("sha") else {})}
        named = {key(m.get("scope"), m.get("target"), m.get("rule_id")) for e in events for m in e.get("marks") or ()}
        for dom, keys in marked.items():
            for k in keys:
                if whole:
                    self.outcomes[k] = {"state": "refused", "why": whole, "domain": dom, "at": at}
                elif k not in named:                       # nothing to land, or landed by a file writer
                    self.outcomes[k] = {"state": "saved", "why": None, "domain": dom, "at": at}
        refused = [e for e in events if e.get("status") == "refused"]
        files = [AP._rel(Path(f)) for f in got.get("files") or ()]
        for dom in domains:
            mine = [e for e in refused if e.get("domain") == dom]
            bad = whole or any(self.outcomes.get(k, {}).get("state") == "refused" for k in marked.get(dom, ()))
            self.domains[dom] = {"state": "refused" if bad else "done", "at": at, "took_s": took, "files": files,
                                 "why": whole or "; ".join(e["why"] for e in mine)[:2000] or None,
                                 "waiting": [e["why"] for e in events
                                             if e.get("status") == "waiting" and e.get("domain") == dom][:20]}
        if whole or refused:
            self._log({"at": at, "domains": domains, "rc": got.get("rc"), "error": err, "took_s": took,
                       "marked": marked, "refused": refused})
            _log.warning("WRITE-THROUGH REFUSED %s: %s", ",".join(domains),
                         whole or "; ".join(f"{e.get('id') or e.get('domain')}: {e['why']}" for e in refused)[:500])

    def _log(self, row: dict) -> None:
        if self.log_path is None:
            return
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
        except OSError:
            _log.exception("write-through: could not log")

    # ------------------------------------------------------------------ read

    def status(self, target: Optional[str] = None) -> dict:
        with self._cv:
            saving = sorted(self._pending) + ([] if not self._busy else ["…"])
        out = {k: v for k, v in self.outcomes.items() if target is None or k.split("|")[1] == target}
        return {"enabled": self.enabled, "saving": bool(saving), "domains": dict(self.domains),
                "outcomes": out,
                "attention": {k: v for k, v in out.items() if v.get("state") in ("refused", "waiting")}}


def from_env(marks: Path, root: Path) -> WriteThrough:
    """The server's write-through: on when it writes the curated marks store (or `CURATION_WRITE_THROUGH=1`)."""
    from pipeline.common.curated import CURATED
    flag = os.environ.get("CURATION_WRITE_THROUGH")
    curated = Path(marks).resolve() == Path(CURATED.regulations.review.marks).resolve()
    enabled = flag == "1" or (flag is None and curated)
    log = os.environ.get("CURATION_WRITE_THROUGH_LOG")
    return WriteThrough(marks, enabled=enabled,
                        log_path=Path(log) if log else root / "data" / "generated" / "review" / "write-through.jsonl")
