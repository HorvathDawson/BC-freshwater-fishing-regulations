"""(T520, owner 2026-11-08: publish from the Mac only) A SAVED INPUT REACHES STAGING BY ITSELF.

When the review app writes a file `pipeline.deliver.sync_inputs` stages — an in-season match (`inseason.save_match`),
a DFO places region or the wording registry (the write-through's dfo apply) — it runs THE Mac path, the same command a
person runs by hand:

    deploy/staging/sync-inputs.sh      resolve (the in-season matches, only when they name another release than
                                       LATEST), publish's refusals (`pipeline.regs.inseason.publish`, via
                                       `sync_inputs`), then put what differs into STAGING R2

So the staged set is exactly what that script puts by hand. Its authority is minimal: staging only (the script
hardcodes the staging bucket; production stays the owner's CONFIRM_PROD step), it never touches git, and a refusal or
a failed upload is never silent — `status()` says it (GET /api/inputs/sync, shown on the In-season tab), the server
log warns, and a JSON line goes to `data/generated/review/inputs-sync.jsonl`.

In the background, debounced (several saves -> one run), one run at a time (in-process; the script's own lock also
serialises it with a hand run and upload.sh). It runs with SYNC_INPUTS_FROM_REVIEW=1: matches resolved on another release
are REFUSED there (re-resolve here, then commit), never rewritten outside `writes.commit`. Each input is staged on its own:
`status()["inputs"]` names the ones that failed (`FAILED <name>: …` lines), so a refused match never hides that the DFO
entries went up. FAILS CLOSED: off unless `CURATION_SYNC_INPUTS=1` (curation-review/run.sh sets it for the live tree).
`CURATION_SYNC_CMD` replaces the command (tests).

SPAWNED, NEVER FORKED (2026-11-09): this process has loaded PROJ (`reuse` maps through pyproj), and PROJ registers a
pthread_atfork child handler that closes its SQLite handle cache. A fork from this multi-threaded server runs that
handler in the child before exec, and it can crash there (SIGSEGV, rc -11, no output: the macOS crash report shows
`fork` -> `_pthread_atfork_child_handlers` -> PROJ `SQLiteHandleCache` -> `sqlite3_log` -> `os_log`). `posix_spawn` runs
no atfork handlers. `subprocess` takes its posix_spawn path only with `cwd=None` and `close_fds=False` (Python fds are
non-inheritable anyway, PEP 446), so the working directory is set by `/bin/sh` in the child instead.
"""
from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import re
import shlex
import subprocess
import threading
import time
from pathlib import Path
from typing import Iterable, Optional

_log = logging.getLogger("uvicorn.error")
DEBOUNCE_S = float(os.environ.get("CURATION_SYNC_DEBOUNCE", "2"))
#: how much of the script's output a status carries
TAIL = 4000


def input_paths() -> list[Path]:
    """The curated / source files `sync_inputs` stages (its INPUTS, by their sources)."""
    from pipeline.common.curated import CURATED, SOURCE
    return [Path(CURATED.dfo_salmon.places), Path(CURATED.dfo_salmon.wording), SOURCE / "bc_hydrometric_stations.json",
            Path(CURATED.inseason.notice_waters)]


def touches(files: Iterable[str | Path], root: Path) -> bool:
    """Whether any written file is one `sync_inputs` stages (a directory input: any file under it)."""
    ins = [p.resolve() for p in input_paths()]
    for f in files:
        p = Path(f)
        p = (p if p.is_absolute() else root / p).resolve()
        if any(p == i or i in p.parents for i in ins):
            return True
    return False


class InputsSync:
    def __init__(self, root: Path, *, enabled: bool, cmd: Optional[list[str]] = None,
                 log_path: Optional[Path] = None, debounce: float = DEBOUNCE_S):
        self.root, self.enabled, self.debounce = Path(root), enabled, debounce
        self.cmd = cmd or [str(self.root / "deploy" / "staging" / "sync-inputs.sh")]
        self.log_path = log_path
        self._cv = threading.Condition()
        self._reasons: list[str] = []
        self._last = 0.0
        self._worker: Optional[threading.Thread] = None
        self.state: dict = {"state": "idle"}

    def request(self, reason: str) -> dict:
        """A save of an input: run the Mac path soon (debounced). Returns the status the response carries."""
        if not self.enabled:
            return {"state": "off", "why": "this server does not write the live curated tree"}
        with self._cv:
            self._reasons.append(reason)
            self._last = time.monotonic()
            self.state = {**self.state, "state": "queued", "queued": sorted(set(self._reasons))}
            if self._worker is None:
                self._worker = threading.Thread(target=self._loop, name="inputs-sync", daemon=True)
                self._worker.start()
            self._cv.notify_all()
        return self.status()

    def _loop(self) -> None:
        while True:
            with self._cv:
                while self._reasons and time.monotonic() - self._last < self.debounce:
                    self._cv.wait(self.debounce)
                if not self._reasons:
                    self._worker = None          # under the lock: a request now starts a new worker, never lost
                    return
                reasons, self._reasons = sorted(set(self._reasons)), []
                self.state = {"state": "running", "reasons": reasons, "at": _now()}
            self._run(reasons)

    def _run(self, reasons: list[str]) -> None:
        t0 = time.monotonic()
        try:
            p = subprocess.run(spawn_argv(self.cmd, self.root), capture_output=True, text=True, timeout=1800,
                               close_fds=False, env={**os.environ, "SYNC_INPUTS_FROM_REVIEW": "1"})
            rc, out = p.returncode, (p.stdout + p.stderr)
        except Exception as ex:                                  # noqa: BLE001 — said, never swallowed
            rc, out = 1, f"{type(ex).__name__}: {ex}"
        took = round(time.monotonic() - t0, 1)
        ok = rc == 0
        failed = {m.group(1): m.group(2) for m in re.finditer(r"^FAILED ([\w-]+): (.*)$", out, re.M)}
        self.state = {"state": "done" if ok else "failed", "at": _now(), "reasons": reasons, "rc": rc,
                      "took_s": took, "failed": failed, "output": out[-TAIL:]}
        if not ok:
            _log.warning("INPUTS SYNC FAILED (%s, rc %s): %s", ",".join(reasons), rc, out[-500:])
            self._log({"at": self.state["at"], "reasons": reasons, "rc": rc, "took_s": took, "output": out[-TAIL:]})

    def _log(self, row: dict) -> None:
        if self.log_path is None:
            return
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        except OSError:
            _log.exception("inputs sync: could not log")

    def wait_idle(self, timeout: float) -> bool:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            w = self._worker
            if w is None or not w.is_alive():
                return True
            w.join(0.1)
        return False

    def status(self) -> dict:
        return {"enabled": self.enabled, **self.state}


def spawn_argv(cmd: list[str], cwd: Path) -> list[str]:
    """`cmd` run in `cwd` by `/bin/sh` (an absolute path, so `subprocess` may posix_spawn it: see the module docstring)."""
    return ["/bin/sh", "-c", 'cd "$0" && exec "$@"', str(cwd), *cmd]


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def from_env(root: Path) -> InputsSync:
    """FAILS CLOSED: on only with `CURATION_SYNC_INPUTS=1` AND the live curated tree (never a `CURATED_ROOT` sandbox)."""
    flag = os.environ.get("CURATION_SYNC_INPUTS")
    cmd = os.environ.get("CURATION_SYNC_CMD")
    return InputsSync(root, enabled=flag == "1" and not os.environ.get("CURATED_ROOT"),
                      cmd=shlex.split(cmd) if cmd else None,
                      log_path=root / "data" / "generated" / "review" / "inputs-sync.jsonl")
