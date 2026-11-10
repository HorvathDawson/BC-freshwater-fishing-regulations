"""Curation-review backend — FastAPI over the pipeline-reuse data layer (see reuse.py).

Run it via the helper (sets PYTHONPATH to the repo root so `import pipeline...` works, and runs from
this dir so `import reuse` works — the folder name has a hyphen so it can't be a package):
    bash curation-review/backend/run.sh          # -> http://127.0.0.1:8787

Makes NO LLM calls — pure local file review. Safe to run any time, including out of parsing credits.

REVIEW MODE ONLY (T134, T185): the app serves reads and TWO write routes — a mark (`POST /api/marks`) and an
in-season match (`POST /api/inseason/matches`, data/curated/inseason/notice_waters.json) — both through `writes.commit`,
every refusal logged (`REFUSED_LOG`). There are no editors (entry save, split add / move / delete / rename — the old
"fix mode" — are gone). A saved mark then reaches its domain's curated file by the WRITE-THROUGH (T325,
`write_through.py`): the file domains (sloughs, names, tidal) in the request, the proposal domains (dfo, entries:
the catalogue and splits.json) in the background, one commit per proposal.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from collections import Counter

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, ConfigDict

import reuse

app = FastAPI(title="Curation Review")


@app.exception_handler(FileNotFoundError)
def _missing_file(_req, ex: FileNotFoundError):
    """(T434) A missing input (a build, a review input) is a 503 that SAYS which and why — never an empty answer. The
    frontend shows `detail` in its error banner."""
    from fastapi.responses import JSONResponse
    return JSONResponse(status_code=503, content={"detail": str(ex)})
#: THE ONLY ORIGIN THE BROWSER MAY CALL FROM: the Vite dev server, which binds 127.0.0.1 only
#: (`frontend/vite.config.ts`). It proxies `/api`, so the app itself never needs CORS; this is what
#: stops another page the reviewer has open from reading the review or posting a mark (T96: it was
#: `*`). `CURATION_VITE_PORT` moves it with Vite's port (a second copy of the tool beside the owner's).
VITE_ORIGIN = f"http://127.0.0.1:{os.environ.get('CURATION_VITE_PORT', '5173')}"
app.add_middleware(
    CORSMiddleware, allow_origins=[VITE_ORIGIN], allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)


@app.middleware("http")
async def _fresh_caches(request, call_next):
    """Before every API request: if the served build or the bundle changed on disk (a promote, or a
    rebuild run from a terminal), drop the caches read from it (`reuse.ensure_fresh`) — a stale
    registry or graph is never served. A few `stat`s; the big loads happen lazily after."""
    if request.url.path.startswith("/api/"):
        reuse.ensure_fresh()
    return await call_next(request)


#: T325: a CLOSED review domain — one that FEEDS NOTHING (ponds, T329) — answers 423 on its queue / progress / detail
#: endpoints (the tab shows a "closed" notice) and a mark on its scopes is refused (`_post_mark`). Every other domain
#: is never closed: its marks reach its final curated file by WRITE-THROUGH (`write_through`, owner 2026-10-30). The
#: closed list is `pipeline.regs.review.closed`, read on each request (a few bytes) beside the marks store.
LOCKED_PATHS = {"/api/q154/": "ponds", "/api/sloughs/queue": "sloughs", "/api/sloughs/progress": "sloughs",
                "/api/sloughs/candidates/": "sloughs", "/api/tidal/queue": "tidal", "/api/tidal/progress": "tidal",
                "/api/tidal/candidates/": "tidal", "/api/dfo/queue": "dfo", "/api/dfo/progress": "dfo"}


def closed_domains() -> dict:
    """domain -> its closed entry (`review.closed`), for the marks store this server writes."""
    from pipeline.regs.review import closed as C
    return C.entries(C.path(reuse.MARKS_PATH))


def _locked_words(domain: str, e: dict) -> str:
    return (f"the {domain} review is closed ({e.get('date')}: {e.get('why')}) — nothing reads its marks. Reopen: "
            f"remove its entry from data/curated/review/closed.json")


@app.middleware("http")
async def _locked_domains(request, call_next):
    path = request.url.path
    dom = next((d for pre, d in LOCKED_PATHS.items() if path.startswith(pre)), None)
    if dom is not None:
        shut = closed_domains()
        if dom in shut:
            from fastapi.responses import JSONResponse
            return JSONResponse({"detail": _locked_words(dom, shut[dom]), "locked": dom}, status_code=423)
    return await call_next(request)


@app.get("/api/review/closed")
def review_closed():
    """T325: the closed review domains (they feed nothing), each with its scopes — the app shows a "closed" notice."""
    return {"closed": closed_domains()}


#: WRITE-THROUGH (T325, owner 2026-10-30): after a mark is appended, its domain's apply runs (`write_through`) — the
#: curated final file follows the marks with no command and no lock. On only over the curated marks store (or
#: `CURATION_WRITE_THROUGH=1`); a refused apply keeps the mark and says why on the item.
import write_through as _wt                                     # noqa: E402
WRITE_THROUGH = _wt.from_env(reuse.MARKS_PATH, reuse._ROOT)
#: (T520, owner 2026-11-08: publish from the Mac only) a saved input `sync_inputs` stages — an in-season match, or a
#: DFO places / wording file the write-through wrote — runs deploy/staging/sync-inputs.sh (`inputs_sync`)
import inputs_sync as _is                                       # noqa: E402
INPUTS_SYNC = _is.from_env(reuse._ROOT)
WRITE_THROUGH.on_files = lambda files: (INPUTS_SYNC.request("write-through")
                                        if _is.touches(files, reuse._ROOT) else None)


@app.on_event("shutdown")
def _drain_write_through():
    """(T434) a background apply in flight finishes before the process goes (a restart once dropped it); (T520) so
    does a staging sync it started."""
    INPUTS_SYNC.wait_idle(60)
    if not WRITE_THROUGH.wait_idle(30):
        import logging
        logging.getLogger("uvicorn.error").warning("WRITE-THROUGH: still applying at shutdown after 30 s; the next "
                                                   "mark re-applies its domain")


def _dfo_apply_outcome(target: str):
    if not WRITE_THROUGH.enabled:          # a sandbox / second copy: nothing applies a save here, and that is said
        return {"state": "off", "why": "the write-through is off on this server (a sandbox or second copy): nothing applies saves here"}
    return WRITE_THROUGH.outcomes.get(_wt.key("dfo", target))


def _apply_stamp() -> tuple:
    return tuple(sorted((k, v.get("state")) for k, v in dict(WRITE_THROUGH.outcomes).items()))


# the checklist reads a place between its mark and the background apply: it asks the write-through what it is doing
import checks as _checks  # noqa: E402
_checks.apply_outcome, _checks.apply_stamp = _dfo_apply_outcome, _apply_stamp


@app.get("/api/review/write-through")
def review_write_through(target: str | None = None):
    """What the write-through did: per domain (saving / done / refused, the files written, proposals waiting on a
    side rebuild) and per marked item (saved / saving / refused / waiting, and why)."""
    return WRITE_THROUGH.status(target)


#: WARM AT STARTUP (T136, PLAN §4 phase 5): `CURATION_WARM=1` (run.sh sets it) loads every cache a
#: first entry reads in a background thread (`reuse.warm`), so the reviewer never waits on a cold
#: backend; `GET /api/ready` says when it is done. Off by default (tests, a one-off script).
WARM = os.environ.get("CURATION_WARM") == "1"


@app.on_event("startup")
def _warm_at_startup():
    if WARM:
        import threading
        threading.Thread(target=reuse.warm, name="curation-warm", daemon=True).start()

        def _health():                    # owner 2026-10-28: say orphaned marks at startup, loudly
            import logging
            try:
                h = reuse.marks_health()
            except Exception:
                logging.getLogger("curation").exception("marks health failed")
                return
            if h["n_orphans"] or h["n_stale"]:
                logging.getLogger("uvicorn.error").warning(
                    "MARKS: %d orphaned, %d stale — %s", h["n_orphans"], h["n_stale"],
                    "; ".join(f"{r['scope']} {r['target']}" for r in (h["orphans"] + h["stale_rows"])[:20]))
            import dfo                    # T366: typed rows older than the saved DFO page, said at startup
            for w in dfo.vintage():
                logging.getLogger("uvicorn.error").warning("DFO VINTAGE: %s", w["text"])
            try:                          # T532 r8: the checklist's places (graph + geometry + every row) before he opens it
                import checks
                checks._places()
            except Exception:
                logging.getLogger("curation").exception("warming the checklist failed")
        threading.Thread(target=_health, name="marks-health", daemon=True).start()


def _repo_head() -> tuple[str | None, str | None]:
    """(HEAD commit, None) of the checkout this backend serves, or (None, why not) — never a silent blank. Read from
    the .git files (HEAD -> its ref, loose or packed; a worktree's `gitdir:` / `commondir`): the backend starts no
    process (`test_curation_review_safety`)."""
    try:
        git = reuse._ROOT / ".git"
        gitdir = git
        if git.is_file():                                        # a worktree: ".git" names its gitdir
            gitdir = Path(git.read_text(encoding="utf-8").split("gitdir:", 1)[1].strip())
            if not gitdir.is_absolute():
                gitdir = (reuse._ROOT / gitdir).resolve()
        common = gitdir
        if (gitdir / "commondir").is_file():
            common = (gitdir / (gitdir / "commondir").read_text(encoding="utf-8").strip()).resolve()
        head = (gitdir / "HEAD").read_text(encoding="utf-8").strip()
        if not head.startswith("ref: "):
            return head, None                                    # detached
        ref = head[5:].strip()
        for base in (gitdir, common):
            if (base / ref).is_file():
                return (base / ref).read_text(encoding="utf-8").strip(), None
        packed = common / "packed-refs"
        if packed.is_file():
            for ln in packed.read_text(encoding="utf-8").splitlines():
                if ln.endswith(" " + ref) and not ln.startswith(("#", "^")):
                    return ln.split()[0], None
        return None, f"HEAD names {ref}, which no ref file or packed-refs holds"
    except (OSError, IndexError) as ex:
        return None, f"{type(ex).__name__}: {ex}"


#: (T541) WHICH BACKEND THIS IS: the commit it started on — run.sh and the app header name it, and the header
#: shows a banner when HEAD has moved since (the code / marks store / write-through a stale copy serves)
_STARTED_COMMIT, _STARTED_GIT_ERROR = _repo_head()


@app.get("/api/ready")
def get_ready():
    """THE READY SIGNAL: whether the backend has loaded what a first entry needs (`reuse.warm`),
    step by step with the time each finished — poll it before the reviewer starts. (T541) `backend`: this
    process's pid, the commit it started on, HEAD now (`head_moved`: they differ; None = git could not say, and
    `git_error` says why), its marks store and whether write-through is on."""
    head, err = _repo_head()
    moved = None if (head is None or _STARTED_COMMIT is None) else head != _STARTED_COMMIT
    return {**reuse.readiness(),
            "backend": {"pid": os.getpid(), "started_commit": _STARTED_COMMIT, "head_now": head,
                        "head_moved": moved, "git_error": _STARTED_GIT_ERROR or err,
                        "marks_path": str(reuse.MARKS_PATH), "write_through": bool(WRITE_THROUGH.enabled)}}


@app.get("/api/marks/health")
def get_marks_health():
    """EVERY OWNER MARK ACCOUNTED FOR (owner 2026-10-28, `marks.health`): per scope held / landed /
    stale / orphan, with the orphaned (target gone) and stale marks listed with what they said. The
    app shows a banner when any mark is orphaned or stale, instead of silently showing it undone."""
    return reuse.marks_health()


@app.get("/api/mode")
def get_mode():
    """What this server is: the app the answer is checked in, the checks each scope answers and the
    problem categories a "wrong" mark names (`marks.PROBLEMS`). Review mode is the only mode."""
    import answer
    from pipeline.regs.review.marks import CHECKS, PROBLEMS
    from pipeline.regs.review.precheck import CHECKS as PRECHECKS
    # the checks a reviewer answers: an entry's, and a zone entry's rule's (a split is no review
    # target — T184 — though old split marks stay readable through GET /api/marks)
    return {"app_url": answer.APP_URL,
            "checks": {k: list(v) for k, v in CHECKS.items() if k in REVIEW_SCOPES},
            "problems": {k: list(v) for k, v in PROBLEMS.items()},
            # T135: the pre-check codes (severity, meaning) and the commands the screen PRINTS —
            # it runs neither (review mode writes one thing, a mark)
            "prechecks": {k: {"severity": sev, "what": what} for k, (sev, what) in PRECHECKS.items()},
            "commands": {"commit_marks": "python -m pipeline.regs.review.commit_marks",
                         # T325: marks land in their curated files by write-through; this is the same apply by
                         # hand (a catch-up after the server was off)
                         "apply": "python -m pipeline.regs.review.apply [domain]",
                         "tickets": "python -m pipeline.regs.review.tickets --out <dir>",
                         "prechecks": "python -m pipeline.regs.review.precheck"}}


@app.get("/api/regions")
def get_regions():
    """Per region: how many entries, by status and by the reviewer's mark. One pass over the one
    queue (T135), grouped — not a queue per region."""
    rows = reuse.queue()
    out = []
    for r in reuse.regions():
        mine = [x for x in rows if x["region"] == r]
        out.append({"id": r, "total": len(mine), "by_status": dict(Counter(x["status"] for x in mine)),
                    "by_verify": dict(Counter(x["verify"] for x in mine))})
    return out


@app.get("/api/entries")
def get_entries(region: str | None = None, status: str | None = None,
                kind: str | None = None, verify: str | None = None,
                order: str = "risk", tier: str | None = None, severity: str | None = None):
    """THE ONE QUEUE (T135): every region unless `region` names one. `kind` is zone | water — the
    region chapters or the water tables. `verify` is the reviewer's mark (unmarked | verified |
    wrong | unsure | deferred | stale | todo | recheck — human-reviewed, changed since); `tier` the
    review tier (PLAN §3); `status` what the parse left (`reuse.STATUSES`); `order` is risk (the
    review order: tier, the most urgent risk factor, the severity-A pre-checks, then the book —
    `tiers.queue_rank`) or book (region, page, then the page's order). `severity` filters on the
    entry's severity (`severity.FILTERS`: high | medium | low | none | issues — high and medium);
    the review order puts it first (owner 2026-10-26)."""
    from pipeline.regs.review.severity import FILTERS
    if severity and severity not in FILTERS:
        raise HTTPException(422, [{"path": "severity",
                                   "msg": "severity is one of " + ", ".join(FILTERS)}])
    if order not in reuse.QUEUE_ORDERS:
        raise HTTPException(422, [{"path": "order",
                                   "msg": "order is " + " or ".join(reuse.QUEUE_ORDERS)}])
    if status and status not in reuse.STATUSES:
        raise HTTPException(422, [{"path": "status",
                                   "msg": "status is one of " + ", ".join(reuse.STATUSES)}])
    rows = reuse.queue(region=region, status=status, kind=kind, verify=verify, order=order,
                       tier=tier, severity=severity)
    import preverify
    # the second method's verdict on the entry's split proposals (owner 2026-10-27), read only
    import awaiting_water
    # owner 2026-10-28: the low-confidence cuts each entry's rules name (the "low-confidence cuts" queue)
    return [{**r, "pv": preverify.entry(r["entry_id"]), "t89": awaiting_water.flag(r["entry_id"]),
             "low_cuts": reuse.low_cuts_of(r["entry_id"]) or None} for r in rows]


#: what a reviewer marks: an entry, or one rule of a zone entry (T184: entries only); T164: a DFO
#: salmon location (the DFO review mode); T212: a proposed tidal boundary (the Tidal mode)
REVIEW_SCOPES = ("entry", "rule", "dfo", "tidal", "slough", "q154", "name")


class MarkPayload(BaseModel):
    """What a reviewer sends; the fingerprints, the date and the book's sha256 are stamped by the
    server (`reuse.post_mark`), never taken from the client."""
    model_config = ConfigDict(extra="forbid")
    scope: str                       # entry | rule (REVIEW_SCOPES)
    target: str = ""                 # entry_id (a slough batch names `targets` instead)
    rule_id: str | None = None       # scope rule
    #: verified | wrong | unsure | deferred; omitted -> `marks.verdict_from_answers` the answers (T134)
    verdict: str | None = None
    answers: dict[str, str]          # T / R / P / A -> y | n | ?
    problem: str | None = None
    note: str = ""
    reviewer: str
    pages: list[int] = []            # printed pages read (default: the entry's source_pages)
    #: the reviewer's time on the target before this mark, by the screen's clock
    seconds: float | None = None
    #: T189: the reviewer's call on tributaries — "entry" or a rule id -> include | exclude
    tributaries: dict[str, str] = {}
    #: T189: the target needs a lake split later (a lake divided into areas; RULINGS K5)
    lake_split_later: bool = False
    #: T191: the reviewer's proposed extent(s) — `marks.ExtentFix` {scope, op, points, items,
    #: tributaries, area?}; validated by the mark model, never applied to a file
    extent_fix: list[dict] = []
    #: T164: NEW splits the reviewer proposes — `marks.NewSplit` {ref, anchor, name?, item_id?,
    #: split_id?, on_item?, coord?, side?, offset_m?, quote, note}; each anchor is checked and its
    #: resolved point stamped by the server; never written to splits.json (T185)
    new_splits: list[dict] = []
    #: T55: the owner's call on a slough / canal candidate — "stream" | "not stream" (scope slough);
    #: Q154: "pool" | "real lake" on a sample small pond (scope q154);
    #: T449: "tidal water" | "not tidal" on a DFO place proposed as wholly tidal (scope dfo)
    call: str | None = None
    #: T55: "accept all visible" — one slough mark per candidate, in one commit (scope slough only)
    targets: list[str] = []
    #: owner 2026-10-29: verdict "cleared" only — clear a target only while its mark in force says
    #: these (verdict / note / date / reviewer / call): an Undo of a batch clears exactly that
    #: batch's marks and skips a target marked again since
    if_last: dict[str, str | None] | None = None
    #: owner 2026-11-02 (scope dfo): this wording is a variant of that DFO location — T185 writes its
    #: `duplicate_of` (confirmed). Its one check is T.
    variant_of: str | None = None
    #: T532 (scope dfo): this wording is NOT the same place as that DFO location — taken out of its group
    #: (`marks.Mark.not_variant_of`); undone by a later `variant_of`. Its one check is T.
    not_variant_of: str | None = None
    #: T525 (scope dfo): the place's extent the owner was shown — its curated extents and its own tributary flag
    #: (`dfo.extent_now`'s `seen`); a DFO mark made on an extent that has moved since is refused (`dfo.mark_for`)
    extent_before: dict | None = None
    #: T532 (scope dfo or rule): the checklist the mark answers — `marks.Checklist` {digest, rows}; refused when its
    #: digest is not the place's now (the words or the extent moved since the owner looked)
    checklist: dict | None = None
    #: owner 2026-11-09 (scope dfo, a group save): every OTHER wording in `targets`, listed with what the owner saw —
    #: {id: {extent_before, digest?}} (its extent; on a checklist its own digest). A wording not listed is refused
    #: (`dfo.marks_for_group`): the server never writes a wording nobody ticked
    members: dict[str, dict] = {}


#: EVERY SAVE THE SERVER REFUSES, on file (owner 2026-10-28: a refused save must be impossible to
#: miss — the terminal is not enough). One JSON line each: when, the item, the status, the reasons
#: and the payload (so it can be retried). `CURATION_REFUSED_LOG` points it at a copy (tests).
REFUSED_LOG: Path | None = None
_REFUSED_DEFAULT = reuse._ROOT / "data" / "generated" / "review" / "refused-saves.jsonl"


def refused_log() -> Path:
    """`REFUSED_LOG` when set, else `CURATION_REFUSED_LOG` read NOW (the tests point every app they
    build at a temp file), else the repo's log."""
    env = os.environ.get("CURATION_REFUSED_LOG")
    return REFUSED_LOG or (Path(env) if env else _REFUSED_DEFAULT)


def _log_refused(body: "MarkPayload | InSeasonMatchPayload", status: int, detail) -> None:
    """One refused save on file; a mark or (T434) an in-season match (scope `inseason`, its row id the target)."""
    import datetime as _dt
    import json as _json
    import logging
    reasons = ([d.get("msg", str(d)) if isinstance(d, dict) else str(d) for d in detail]
               if isinstance(detail, list) else [str(detail)])
    row = {"at": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
           "scope": getattr(body, "scope", "inseason"), "target": body.target if hasattr(body, "scope") else body.id,
           "rule_id": getattr(body, "rule_id", None), "targets": list(getattr(body, "targets", ())),
           "status": status, "reasons": reasons,
           "reviewer": getattr(body, "reviewer", None) or getattr(body, "by", ""), "payload": body.model_dump()}
    try:
        import writes
        writes.log_refused(refused_log(), _json.dumps(row, ensure_ascii=False, default=str))
    except OSError:
        logging.getLogger("uvicorn.error").exception("could not log a refused save")
    logging.getLogger("uvicorn.error").warning("REFUSED SAVE %s %s (%s): %s", row["scope"],
                                               row["target"], status, "; ".join(reasons))


@app.post("/api/marks")
def post_mark(body: MarkPayload):
    """A reviewer's mark (one of the app's two writes), with every refusal logged (`REFUSED_LOG`) before it is
    returned."""
    try:
        return _post_mark(body)
    except HTTPException as ex:
        _log_refused(body, ex.status_code, ex.detail)
        raise
    except Exception as ex:
        _log_refused(body, 500, f"{type(ex).__name__}: {ex}")
        raise


def _post_mark(body: MarkPayload):
    """Record a reviewer's mark (T97) in the append-only store `marks.jsonl`, against the
    target's fingerprints NOW. THE ONE WRITE OF REVIEW MODE. Refused (422) on an entry that fails
    the gate, or a mark the model refuses (a verdict its answers do not support, no reviewer);
    404 for an unknown target. With no `verdict` the answers decide it (`marks.verdict_from_answers`)."""
    from pydantic import ValidationError
    from pipeline.regs.review.marks import (CHECKS, refusal_words, unanswered_words,
                                            verdict_from_answers)
    data = body.model_dump()
    from pipeline.regs.review.export import DOMAINS
    shut = closed_domains()
    dom = next((d for d in shut if body.scope in DOMAINS.get(d, ())), None)
    if dom is not None:                              # T325: a closed domain takes no mark (not even a clear)
        raise HTTPException(423, [{"path": "scope", "msg": _locked_words(dom, shut[dom])}])
    if body.verdict == "cleared":
        # owner 2026-10-29: TAKE BACK the mark in force (`marks.cleared_mark`) — any review scope, one
        # target or several (`targets`), one commit; the target reads unmarked again
        if body.scope not in REVIEW_SCOPES:
            raise HTTPException(422, [{"path": "scope", "msg": f"scope is one of {list(REVIEW_SCOPES)}"}])
        try:
            got = reuse.clear_marks(data)
        except ValidationError as ex:
            raise HTTPException(422, [{"path": "", "msg": refusal_words(
                e["msg"], str(e["loc"][0]) if e["loc"] else None)} for e in ex.errors()])
        except ValueError as ex:
            raise HTTPException(422, [{"path": "", "msg": refusal_words(str(ex))}])
        return _written_through(got, body)
    if body.if_last is not None:
        raise HTTPException(422, [{"path": "", "msg": "only a cleared mark says which mark it clears"}])
    data.pop("if_last")
    if body.scope != "dfo":                         # owner 2026-11-02: only a DFO wording is a variant
        if body.variant_of:
            raise HTTPException(422, [{"path": "variant_of", "msg": "only a DFO wording is attached as a variant"}])
        data.pop("variant_of")
    elif not body.variant_of:
        data.pop("variant_of")
    if body.scope != "dfo" and body.not_variant_of:
        raise HTTPException(422, [{"path": "not_variant_of", "msg": "only a DFO wording is taken out of a group"}])
    if not body.not_variant_of:
        data.pop("not_variant_of")
    if body.scope != "dfo":
        if body.extent_before is not None:
            raise HTTPException(422, [{"path": "extent_before",
                                       "msg": "only a DFO mark names the extent it was made on"}])
        data.pop("extent_before")
    elif body.extent_before is None:
        data.pop("extent_before")
    if body.scope != "dfo":
        if body.members:
            raise HTTPException(422, [{"path": "members", "msg": "only a DFO save lists older wordings"}])
        data.pop("members")
    if body.checklist is None:
        data.pop("checklist")
    else:
        _refuse_stale_checklist(body)
    if body.scope == "q154":
        if body.targets:
            raise HTTPException(422, [{"path": "", "msg": "a Q154 mark names one sample pond"}])
        data.pop("targets")
    elif body.scope == "dfo":                       # owner 2026-10-28: a historic group's members
        # T449: the one DFO call — a place proposed as wholly in tidal water, accepted or rejected
        if body.call not in (None, "tidal water", "not tidal"):
            raise HTTPException(422, [{"path": "call", "msg": "a DFO call is tidal water or not tidal"}])
        if body.call is None:
            data.pop("call")
        if not body.targets:
            data.pop("targets")
            if body.members:
                raise HTTPException(422, [{"path": "members", "msg": "older wordings are listed only with the "
                                           "targets they are saved on"}])
        if not body.members:
            data.pop("members")
    elif body.scope == "tidal":                     # T359: "the whole water is tidal" is a tidal call
        if body.targets:
            raise HTTPException(422, [{"path": "", "msg": "a tidal mark names one candidate"}])
        if body.call not in (None, "whole"):
            raise HTTPException(422, [{"path": "call", "msg": "a tidal call is whole (the whole water is tidal)"}])
        data.pop("targets")
        if body.call is None:
            data.pop("call")
    elif body.scope not in ("slough", "name"):     # T224: a name mark may name several candidates too
        if body.targets or body.call is not None:
            raise HTTPException(422, [{"path": "", "msg": "only a slough mark carries a call or several targets"}])
        data.pop("targets"), data.pop("call")
    if body.scope not in REVIEW_SCOPES:
        raise HTTPException(422, [{"path": "scope", "msg": (
            "A split is not marked on its own: check it on the map of an entry whose rules name "
            "it, and mark that entry's Place wrong if the cut is in the wrong place."
            if body.scope == "split" else
            # T185 (owner 2026-10-27): the marks ARE the decisions; a proposal has no accept step
            "A proposal is not accepted here: the marks behind it are the decision. Change the "
            "mark to change the proposal." if body.scope == "proposal" else
            f"scope is one of {list(REVIEW_SCOPES)}")}])
    # A REFUSAL IS SAID IN THE REVIEWER'S WORDS (T142, `marks.refusal_words`): what to do, never
    # the model's "Value error, …" — and with no field path, which means nothing at the screen.
    if data["verdict"] is None:
        data["verdict"] = verdict_from_answers(body.scope, body.answers)
        if data["verdict"] is None:
            raise HTTPException(422, [{"path": "", "msg": unanswered_words(
                body.scope, awaiting=reuse.awaiting_build(body.scope, body.target))}])
    try:
        got = reuse.post_mark(data)
    except KeyError as ex:
        raise HTTPException(404, f"{body.scope} {body.target}"
                                 f"{'/' + body.rule_id if body.rule_id else ''} not found: {ex}")
    except ValidationError as ex:
        raise HTTPException(422, [{"path": "", "msg": refusal_words(
            e["msg"], str(e["loc"][0]) if e["loc"] else None)} for e in ex.errors()])
    except ValueError as ex:
        raise HTTPException(422, [{"path": "", "msg": refusal_words(str(ex))}])
    return _written_through(got, body)


def _refuse_stale_checklist(body: MarkPayload) -> None:
    """T532: a checklist is saved only on the rows the place has NOW (same digest) — never on words or an extent that
    moved since the owner looked, and never for a group (each member has its own rows)."""
    import checks
    if body.scope == "dfo":
        # owner 2026-11-09: a check on the older wordings the owner ticked "same place" — each one's own digest NOW
        # (its words or extent moved since: refused by name) and none whose own save still waits to land
        import dfo
        for t in dict.fromkeys(body.targets):
            if t == body.target:
                continue
            got = checks.dfo_check(t)
            if got is None:
                raise HTTPException(404, f"dfo {t} not found")
            words = (dfo._locations()[t][1].get("source_text") or {}).get("specific_area") or t
            if got.get("held"):
                raise HTTPException(422, [{"path": "targets", "msg": f"older wording “{words}”: {got['held']}"}])
            if (body.members.get(t) or {}).get("digest") != got["digest"]:
                raise HTTPException(422, [{"path": "targets", "msg": f"older wording “{words}”: its words or its "
                                           "extent changed since you opened it: reload the place and check it again "
                                           "— nothing was saved"}])
        now = checks.dfo_check(body.target)
    elif body.scope == "rule":
        now = checks.rule_check(body.target, body.rule_id or "")
    else:
        raise HTTPException(422, [{"path": "checklist", "msg": "a checklist is on a DFO place or a regulation rule"}])
    if now is None:
        raise HTTPException(404, f"{body.scope} {body.target} not found")
    if now.get("held"):
        raise HTTPException(422, [{"path": "checklist", "msg": now["held"]}])
    if (body.checklist or {}).get("digest") != now["digest"]:
        raise HTTPException(422, [{"path": "checklist", "msg": "its words or its extent changed since you opened it: "
                                   "reload the place and check it again — nothing was saved"}])


def _written_through(got, body: MarkPayload):
    """The mark is in the store: run its domain's apply (T325 write-through; one per request, a batch included) and
    say what it did. Never raises — a refused apply keeps the mark and is said on the item."""
    try:
        wt = WRITE_THROUGH.after_mark(body.scope, list(dict.fromkeys(
            ([body.target] if body.target else []) + list(body.targets))), body.rule_id)
    except Exception as ex:                                     # noqa: BLE001 — the mark is saved regardless
        import logging
        logging.getLogger("uvicorn.error").exception("write-through failed")
        wt = {"state": "refused", "why": f"{type(ex).__name__}: {ex}"}
    if isinstance(got, dict):
        return {**got, "write_through": wt}
    return got


@app.get("/api/marks")
def get_marks(target: str | None = None, scope: str | None = None):
    """The marks in force (the last one on each target), each with its `status` and typed stale
    `reasons` against its target as it is now.
    PROBE (T434): no screen calls it; kept as the tests' and the owner's debug read."""
    return reuse.marks_view(target, scope)


@app.get("/api/marks/splits")
def get_split_marks():
    """Every split in splits.json -> its old split mark's status now (`unmarked` for most). Split
    marks are HISTORY (T184): none is taken any more, the ones in the store stay readable.
    PROBE (T434): no screen calls it; kept as the tests' and the owner's debug read."""
    return reuse.split_mark_states()


@app.get("/api/build")
def pending_build():
    """THE PENDING BUILD (T133, PLAN §5): every entry and split awaiting its build, why, the
    vintage on disk, and the command(s) the human runs — the tool never runs them. Re-read from
    disk on every request, so it empties when the rebuild lands, with no restart."""
    return reuse.pending_build()


# --------------------------------------------------------------------------- the review screen

@app.get("/api/review/progress")
def review_progress():
    """Done / to do per tier (PLAN §3) and overall — the progress bar; the ETA is the screen's,
    from the reviewer's own pace."""
    import review
    return review.progress()


@app.get("/api/review/ids")
def review_ids(path: str = ""):
    """ONLY THESE ENTRIES (T414): the ids file `path` names (the yearly diff's `--ids-out`, under the
    repo), its progress over the list, and every id that matches no entry (`idlist.view`). A file
    that cannot be read is a 422 that says why — never an empty list."""
    import idlist
    import review
    try:
        return idlist.view(path, reuse._ROOT, reuse.queue(), review.DONE)
    except idlist.Refused as ex:
        raise HTTPException(422, [{"path": "path", "msg": str(ex)}]) from ex


@app.get("/api/review/p1")
def review_p1():
    """THE P1 PASS (owner 2026-10-28, `p1.view`): counts by risk and by the second check, the
    quick-confirm rows (every check agrees) and the rest of P1 in risk order. Read only."""
    import p1
    return p1.view()


@app.get("/api/review/tickets")
def review_tickets():
    """THE TICKET EXPORT (T135): every wrong and unsure mark in force as ticket text, with what
    changed since. Read only — the owner files them (`python -m pipeline.regs.review.tickets
    --out <dir>` writes the same text to files).
    PROBE (T434): no screen calls it; kept as the tests' and the owner's debug read."""
    import review
    return review.tickets()


@app.get("/api/review/tickets.md")
def review_tickets_md():
    """The tickets as one Markdown text (`tickets.SEPARATOR` between them), to read or save."""
    import review
    from pipeline.regs.review.tickets import SEPARATOR
    got = review.tickets()
    body = SEPARATOR.join(t["text"] for t in got) if got else "No wrong or unsure marks in force.\n"
    return Response(body, media_type="text/markdown; charset=utf-8")


@app.get("/api/entries/{entry_id}/review")
def entry_review(entry_id: str, date: str | None = None):
    """What the T/R/P/A checklist is answered from: the raw name, where the entry is printed and a
    legible crop of it, the coverage strip, its tier and risk factors, each zone rule's impact,
    and the app's own page for the answer (a start link)."""
    import review
    try:
        d = review.review_view(entry_id, date)
    except ValueError as ex:
        raise HTTPException(422, [{"path": "date", "msg": str(ex)}])
    if d is None:
        raise HTTPException(404, f"entry {entry_id} not found")
    return d


def _located(entry_id: str) -> dict:
    import evidence
    import review
    try:
        where = review.located(entry_id)
    except KeyError:
        raise HTTPException(404, f"entry {entry_id} not found")
    except evidence.Missing as ex:
        raise HTTPException(404, str(ex))
    if where["kind"] not in ("row", "passage"):
        raise HTTPException(404, where.get("why", "not located"))
    return where


@app.get("/api/entries/{entry_id}/evidence/crop.png")
def evidence_crop(entry_id: str):
    """The entry as printed, LEGIBLE: its row (the water column above the regulation column) or its
    passage, rendered from the repo PDF at 2.6 px/pt."""
    import evidence
    return Response(evidence.crop_png(_located(entry_id)), media_type="image/png")


@app.get("/api/entries/{entry_id}/evidence/page.png")
def evidence_page(entry_id: str):
    """The whole printed page with the entry's row or passage marked."""
    import evidence
    return Response(evidence.page_png(_located(entry_id)), media_type="image/png")


# --------------------------------------------------------------------------- the bundle (reads)

@app.get("/api/entries/{entry_id}/bundle")
def entry_bundle(entry_id: str):
    """The live bundle's copy of this entry beside the file's — per-rule label and section count
    (the bundle's, and the builder's now), where each licensing record binds — and whether they
    match. For an entry AWAITING BUILD only its `vintage` (the bundle's copy is not this entry's).
    PROBE (T434): no screen calls it; kept as the tests' and the owner's debug read."""
    d = reuse.entry_answer_context(entry_id)
    if d is None:
        raise HTTPException(404, f"entry {entry_id} not found")
    return d


@app.get("/api/entries/{entry_id}/answer/waters")
def entry_answer_waters(entry_id: str, q: str = "", limit: int = 60):
    """The waters this entry reaches in the bundle, split into pieces that get one answer each.
    409 while the entry awaits its build (T133): the bundle's answer is another vintage's.
    PROBE (T434): no screen calls it; kept as the tests' and the owner's debug read."""
    import answer
    from pipeline.regs.review.vintage import CURRENT
    found = reuse._find(entry_id)
    if found is None:
        raise HTTPException(404, f"entry {entry_id} not found")
    vin = reuse.entry_vintage(found[1])
    if vin["state"] != CURRENT:
        raise HTTPException(409, {"vintage": vin})
    return answer.entry_waters(entry_id, q=q, limit=limit)


@app.get("/api/answer")
def get_answer(sid: int, date: str, fish: str):
    """The EFFECTIVE answer an angler gets: `read.effective_rules` on one bundle section, one day,
    one fish — plus the licensing bound there and whether it is tidal. Reflects the live bundle.
    PROBE (T434): no screen calls it; kept as the tests' and the owner's debug read."""
    import answer
    try:
        return answer.effective(sid, date, fish)
    except ValueError as ex:
        raise HTTPException(422, [{"path": "fish" if "fish" in str(ex) or "group" in str(ex)
                                   else "date", "msg": str(ex)}])


@app.get("/api/entries/{entry_id}")
def get_entry(entry_id: str):
    d = reuse.entry_detail(entry_id)
    if d is None:
        raise HTTPException(404, f"entry {entry_id} not found")
    import awaiting_water
    import preverify
    # the entry's missing-split / ambiguous-cut proposals with their pre-verify, and its T89
    # awaiting-water record (leads, candidates, fix path) -- all read only
    # LAKE SPLIT LATER (owner 2026-10-28): a proposal for a lake's undrawn part / unresolved area
    # needs a lake split, which is deferred -- flagged so the screen folds it
    from pipeline.regs.review.severity import LAKE_LATER_CODES
    lake = ((d.get("item") or {}).get("kind")) == "lake"
    sugg = [{**i, "lake_later": lake and i.get("kind") in LAKE_LATER_CODES or
             (lake and i.get("kind") == "unplaced")} for i in preverify.entry_items(entry_id)]
    return {**d, "split_suggestions": sugg,
            "awaiting_water": awaiting_water.proposal(entry_id)}


@app.get("/api/items/{item_id}/geojson")
def item_geojson(item_id: str):
    """Stream sections + curated split points for the map, in EPSG:4326 (lon/lat) to overlay the
    web-mercator PMTiles basemap. Selected by the item's own section_ids / curated split ids."""
    return reuse.item_geojson(item_id)


@app.get("/api/items/{item_id}/tributaries/geojson")
def item_tributary_geojson(item_id: str, limit: int = 400):
    """One level of tributaries as map geometry — what `includes_tributaries` actually covers."""
    return reuse.item_tributary_geojson(item_id, limit)


@app.get("/api/entries/{entry_id}/rules/{rule_id}/resolved")
def rule_resolved(entry_id: str, rule_id: str, limit: int = 6000):
    """What this rule covers and what it excepts — the reach builder's own answer, as geometry.

    Covers the two shapes the item layer cannot draw: tributary expansion, and `within(area)`
    (Pitt River in Garibaldi binds 466 sections, 18 of them the Pitt's own).

    Button-driven: the resolve is cheap, the geometry is not — the largest rule is ~3,200
    sections, ~4s and ~12 MB. Not something to pay on every entry open.
    """
    return reuse.rule_resolved_reach(entry_id, rule_id, limit=limit)


@app.get("/api/entries/{entry_id}/reaches")
def entry_reaches(entry_id: str):
    """Per-rule, per-extent section ids — the REACH each rule selects, for map highlighting."""
    return reuse.entry_reaches(entry_id)


@app.get("/api/entries/{entry_id}/splits")
def entry_splits(entry_id: str):
    """EVERY SPLIT THE ENTRY'S RULES NAME, as labelled map points (T184: a split is reviewed by
    proxy, on the map of each entry that uses it) — `reuse.entry_split_points`; the ones the served
    atlas cannot place are listed in `unplaced` with why."""
    got = reuse.entry_split_points(entry_id)
    if got is None:
        raise HTTPException(404, f"entry {entry_id} not found")
    return got


# --------------------------------------------------------------------------- T191: extent fix
# All READS: the editor proposes an extent INTO THE MARK (`Mark.extent_fix`); nothing here writes.

@app.get("/api/extent-fix/ops")
def extent_fix_ops():
    """The ops a fix can propose — the catalogue's own names, their point counts, the owner's
    words — and the point anchor kinds."""
    from typing import get_args
    from pipeline.regs.review import marks as M
    return {"ops": [{"op": k, "points": n, "words": M.OP_WORDS[k]} for k, n in M.OP_POINTS.items()],
            "point_kinds": list(get_args(M.PointKind))}


@app.get("/api/splits/search")
def splits_search(q: str = "", limit: int = 30):
    """Curated splits (splits.json) whose id, label or water contains `q`."""
    return reuse.search_splits(q, limit=limit)


@app.get("/api/areas/search")
def areas_search(q: str = "", limit: int = 30):
    """The atlas's areas whose id or name contains `q` — the "within an area" op's pick list."""
    return reuse.search_areas(q, limit=limit)


@app.get("/api/items/search")
def items_search(q: str = "", kind: str | None = None, limit: int = 30):
    """Registry items whose name contains `q` (a tributary, a lake, an item the extent holds on)."""
    return reuse.search_items(q, kind=kind, limit=limit)


@app.get("/api/entries/{entry_id}/extent-preview")
def extent_preview(entry_id: str, fix: str, limit: int = 6000):
    """THE PROPOSED EXTENT RESOLVED, READ ONLY (`reuse.extent_preview`): `fix` is one
    `marks.ExtentFix` as JSON. Resolved by the builder's own `build_reach` on an in-memory copy of
    the entry; a NEW point (not yet a split) cannot be resolved until T185 builds it, and the
    answer says so (`resolvable: false`, `why`)."""
    import json as _json
    from pydantic import ValidationError
    from pipeline.regs.review import marks as M
    try:
        f = M.ExtentFix.model_validate(_json.loads(fix))
    except (ValueError, ValidationError) as ex:
        raise HTTPException(422, str(ex))
    got = reuse.extent_preview(entry_id, f, limit=limit)
    if got is None:
        raise HTTPException(404, f"entry {entry_id} not found")
    return got


# --------------------------------------------------------------------------- T164: DFO review mode
# All READS: the DFO salmon locations, their map, and a proposed new split's anchor resolved. A
# DFO mark (scope "dfo") is posted through the one write, POST /api/marks.

@app.get("/api/dfo/queue")
def dfo_queue(region: str | None = None, status: str | None = None, need: str | None = None,
              verify: str | None = None, q: str | None = None, lake: str | None = None):
    """THE DFO QUEUE: every scraped DFO location (`dfo.queue`), filtered — `status` active |
    dormant, `need` needs_cut | bound | unbound | no_water, `verify` the mark (todo = unmarked or
    stale), `q` a substring of the water or the notice's words."""
    import dfo
    if need and need not in dfo.NEEDS:
        raise HTTPException(422, [{"path": "need", "msg": "need is one of " + ", ".join(dfo.NEEDS)}])
    if verify and verify not in dfo.VERIFIES:
        raise HTTPException(422, [{"path": "verify",
                                   "msg": "verify is one of " + ", ".join(dfo.VERIFIES)}])
    if lake and lake not in ("later", "all"):
        raise HTTPException(422, [{"path": "lake", "msg": "lake is later | all (default: lake split later hidden)"}])
    return dfo.queue(region=region, status=status, need=need, verify=verify, q=q, lake=lake)


# --------------------------------------------------------------------------- T532: the checklist (reads)

@app.get("/api/checks/queue")
def checks_queue(source: str = "dfo", filter: str | None = None, q: str | None = None):
    """THE CHECKLIST QUEUE (`checks.queue`): every place of `source` (dfo | regs | all) grouped by water, each with
    its rows' summary and its check state; `filter` (todo | unchecked | stale | checked | wrong | bad | unsure) keeps
    every place of a water any of whose places matches."""
    import checks
    try:
        return checks.queue(source, filter, q)
    except ValueError as ex:
        raise HTTPException(422, [{"path": "", "msg": str(ex)}])


@app.get("/api/checks/progress")
def checks_progress(source: str = "dfo"):
    """Checked / total over the whole list, by check state and worst row, per water (`checks.progress`)."""
    import checks
    return checks.progress(source)


@app.get("/api/checks/water")
def checks_water(place: str, source: str = "dfo"):
    """Every place on the water `place` (a queue place or an older wording) is on, older wordings too, each with its
    extent and its same-place group (`checks.water`)."""
    import checks
    if source not in ("dfo", "regs", "all"):
        raise HTTPException(422, "source is dfo, regs or all")
    key = checks.water_key_of(place)
    got = checks.water(key, source) if key else None
    if got is None and key and source != "all":        # a place of the other source, opened by its link
        got = checks.water(key, "all")
    if got is None and not key:                        # a DFO place the queue leaves out (a lake sub-area), by its link
        got = checks.water_alone(place)
    if got is None:
        raise HTTPException(404, f"no water in the checklist holds {place}")
    return got


@app.get("/api/checks/water/reach")
def checks_water_reach(key: str):
    """What each DFO extent on one water binds, as geometry (`checks.water_reach`)."""
    import checks
    got = checks.water_reach(key)
    if got is None:
        raise HTTPException(404, f"no DFO place on water {key}")
    return got


@app.get("/api/checks/dfo/{location_id:path}")
def checks_dfo(location_id: str):
    """One DFO place's checklist rows (`checks.dfo_check`)."""
    import checks
    got = checks.dfo_check(location_id)
    if got is None:
        raise HTTPException(404, f"DFO location {location_id} not found")
    return got


@app.get("/api/checks/rule/{entry_id}/{rule_id}")
def checks_rule(entry_id: str, rule_id: str):
    """One regulation rule's checklist rows (`checks.rule_check`)."""
    import checks
    got = checks.rule_check(entry_id, rule_id)
    if got is None:
        raise HTTPException(404, f"rule {entry_id}/{rule_id} not found")
    return got


@app.get("/api/dfo/progress")
def dfo_progress():
    """Marked / total for the DFO queue, overall, live-page only and per region."""
    import dfo
    return dfo.progress()


@app.get("/api/dfo/cascade-regions")
def dfo_cascade_regions():
    """Every region's DFO cascade (`region_cascade.regions`): the page's region-wide scopes and section
    banners as an ordered tree, the code's defaults beside them, proposals quoting DFO — read only."""
    import region_cascade
    return region_cascade.regions()


@app.get("/api/dfo/cascade-regions/{slug}/map")
def dfo_cascade_region_map(slug: str, min_order: int = 4, tol: float = 60.0):
    """One region's outline and each scope's coverage (`region_cascade.region_map`), plus its check."""
    import region_cascade
    try:
        return region_cascade.region_map(slug, min_order=min_order, tol=tol)
    except KeyError:
        raise HTTPException(404, f"no DFO region {slug!r}")


@app.get("/api/dfo/cascade/{name}")
def dfo_cascade(name: str, min_order: int = 3, tol: float = 30.0):
    """T229 THE CASCADE of one watershed (`cascade.cascade`): every section coloured by the DFO
    places whose chain covers it, narrowest first, and per chain the place that answers each
    species on each day — the bundle's own `verdict_on`, read only."""
    import cascade
    try:
        return cascade.cascade(name, min_order=min_order, tol=tol)
    except KeyError:
        raise HTTPException(404, f"no cascade frame {name!r} (one of: {', '.join(cascade.FRAMES)})")


@app.get("/api/dfo/locations/{location_id:path}/reach")
def dfo_reach(location_id: str, limit: int = 6000, tributaries: bool = False):
    """What the location's curated extent binds NOW (`build_reach`), as geometry. The tributary
    walk is skipped unless `tributaries=true` (the Extents map's "Show tributaries" toggle)."""
    import dfo
    got = dfo.reach(location_id, limit=limit, tributaries=tributaries)
    if got is None:
        raise HTTPException(404, f"DFO location {location_id} not found")
    return got


@app.get("/api/dfo/locations/{location_id:path}/anchor")
def dfo_anchor(location_id: str, split: str):
    """A PROPOSED NEW SPLIT's anchor resolved, READ ONLY (`dfo.check_anchor`): `split` is one
    `marks.NewSplit` as JSON. 422 for a malformed one; `ok: false` and why when it does not land."""
    import json as _json
    import dfo
    from pydantic import ValidationError
    from pipeline.regs.review import marks as M
    try:
        ns = M.NewSplit.model_validate(_json.loads(split))
    except (ValueError, ValidationError) as ex:
        raise HTTPException(422, [{"path": "split", "msg": M.refusal_words(
            ex.errors()[0]["msg"] if isinstance(ex, ValidationError) else str(ex))}])
    try:
        return dfo.check_anchor(location_id, ns)
    except KeyError:
        raise HTTPException(404, f"DFO location {location_id} not found")


@app.get("/api/split-anchors")
def split_anchors():
    """T355: what a new split may be — the area layers (and the field each names its polygons by) and
    the anchor kinds the build can offset (`marks.AREA_LAYERS`, `marks.OFFSET_ANCHORS`)."""
    from pipeline.regs.review import marks as M
    return {"area_layers": M.AREA_LAYERS, "offset_anchors": list(M.OFFSET_ANCHORS)}


def _new_split_param(split: str):
    import json as _json
    from pydantic import ValidationError
    from pipeline.regs.review import marks as M
    try:
        return M.NewSplit.model_validate(_json.loads(split))
    except (ValueError, ValidationError) as ex:
        raise HTTPException(422, [{"path": "split", "msg": M.refusal_words(
            ex.errors()[0]["msg"] if isinstance(ex, ValidationError) else str(ex))}])


@app.get("/api/dfo/locations/{location_id:path}/split-preview")
def dfo_split_preview(location_id: str, split: str, others: str | None = None):
    """T355: A PROPOSED SPLIT'S CUT, READ ONLY, by the build's own resolver (`split_preview.resolve`):
    {ok, why, split_id (the real id it lands as), record, cuts [{blk, measure, coord}], reference, outline,
    words}. `others`: the mark's other new splits (JSON list). 422 for a malformed split."""
    import json as _json
    import dfo
    ns = _new_split_param(split)
    rest = tuple(_new_split_param(_json.dumps(o)) for o in (_json.loads(others) if others else []))
    try:
        return dfo.preview_split(location_id, ns, rest)
    except KeyError:
        raise HTTPException(404, f"DFO location {location_id} not found")


@app.get("/api/dfo/locations/{location_id:path}/tributaries")
def dfo_tributaries(location_id: str, on_item: str | None = None):
    """T355: the streams whose mouth joins the water (a confluence split's pick list), with their mouths."""
    import dfo
    import split_preview
    try:
        items = [on_item] if on_item else dfo.water_items(location_id)[:1]
    except KeyError:
        raise HTTPException(404, f"DFO location {location_id} not found")
    return split_preview.tributaries(items[0]) if items else []


@app.get("/api/dfo/locations/{location_id:path}/area-names")
def dfo_area_names(location_id: str, layer: str = "parks_bc", q: str = "", on_item: str | None = None):
    """T355: an area_boundary's name, as you type — up to 15 names of `layer` containing `q`, prefix
    first, then nearest the water (`split_preview.area_names`), spelled as the layer spells them."""
    import dfo
    import split_preview
    try:
        items = [on_item] if on_item else dfo.water_items(location_id)[:1]
    except KeyError:
        raise HTTPException(404, f"DFO location {location_id} not found")
    try:
        return split_preview.area_names(layer, q, items[0] if items else None)
    except ValueError as ex:
        raise HTTPException(422, [{"path": "layer", "msg": str(ex)}])


@app.get("/api/dfo/locations/{location_id:path}/split-pick")
def dfo_split_pick(location_id: str, kind: str, lon: float, lat: float, on_item: str | None = None):
    """T355: what a map click names for a new split — `kind` tributary | lake | area (`split_preview.pick`)."""
    import dfo
    import split_preview
    try:
        items = [on_item] if on_item else dfo.water_items(location_id)[:1]
    except KeyError:
        raise HTTPException(404, f"DFO location {location_id} not found")
    return split_preview.pick(kind, items[0] if items else None, (lon, lat))


@app.get("/api/dfo/locations/{location_id:path}/extent-preview")
def dfo_extent_preview(location_id: str, fix: str, limit: int = 6000, tributaries: bool = False):
    """A proposed extent (`marks.ExtentFix`) on a DFO location resolved, READ ONLY.
    T524: `fix` may be a LIST — the place's pieces, resolved as their union (`marks.pieces_ok` refuses what a mark
    would refuse)."""
    import json as _json
    import dfo
    from pydantic import ValidationError
    from pipeline.regs.review import marks as M
    try:
        raw = _json.loads(fix)
        f = ([M.ExtentFix.model_validate(x) for x in raw] if isinstance(raw, list)
             else M.ExtentFix.model_validate(raw))
        if isinstance(f, list):
            if not f:
                raise ValueError("no piece to preview")
            M.pieces_ok(f)
    except (ValueError, ValidationError) as ex:
        raise HTTPException(422, str(ex))
    got = dfo.extent_preview(location_id, f, limit=limit, tributaries=tributaries)
    if got is None:
        raise HTTPException(404, f"DFO location {location_id} not found")
    return got


@app.get("/api/dfo/locations/{location_id:path}")
def dfo_location(location_id: str):
    """One DFO location: the notice's words verbatim, its scraped rows, its curated extent, the
    cuts on its water as labelled map points, anchor hints and the mark in force."""
    import dfo
    got = dfo.detail(location_id)
    if got is None:
        raise HTTPException(404, f"DFO location {location_id} not found")
    return got


# --------------------------------------------------------------------------- T212: Tidal review mode
# All READS: the proposed tidal boundaries (`data/generated/review/tidal/tidal-candidates.json`)
# and the polygon's edge near each. Accepting one is a mark (scope "tidal") through POST /api/marks.

@app.get("/api/tidal/queue")
def tidal_queue(confidence: str | None = None, verify: str | None = None, enters: str | None = None,
                words: bool = False, q: str | None = None, triage: str | None = None, followup: bool = False):
    """THE TIDAL QUEUE: every proposed tidal boundary (`tidal.queue`), filtered — `followup` only the marks the
    tidal writer leaves out of tidal.json (T359, each with its `followup_why`), `confidence` high |
    medium | low, `verify` the mark (todo = unmarked or stale), `enters` ocean | tidal_water, `words`
    only those a text names, `q` a substring of the stream's name, `triage` a triage class or
    "review" (missed + sanity: what the owner still has to look at)."""
    import tidal
    if confidence and confidence not in tidal.CONFIDENCES:
        raise HTTPException(422, [{"path": "confidence",
                                   "msg": "confidence is one of " + ", ".join(tidal.CONFIDENCES)}])
    if verify and verify not in tidal.VERIFIES:
        raise HTTPException(422, [{"path": "verify", "msg": "verify is one of " + ", ".join(tidal.VERIFIES)}])
    if triage and triage not in tidal.TRIAGES:
        raise HTTPException(422, [{"path": "triage", "msg": "triage is one of " + ", ".join(tidal.TRIAGES)}])
    return tidal.queue(confidence=confidence, verify=verify, enters=enters, words=words, q=q, triage=triage,
                       followup=followup)


@app.get("/api/tidal/progress")
def tidal_progress():
    """Marked / total for the tidal queue, by confidence, and the proposal file's coverage."""
    import tidal
    return tidal.progress()


@app.get("/api/tidal/candidates/{cid:path}/snap")
def tidal_snap(cid: str, lon: float, lat: float):
    """A clicked point SNAPPED to the candidate's stream (read only): {coord, blk, measure,
    above_mouth_m, distance_m}. 422 when the point is not on the stream (> 500 m)."""
    import tidal
    got = tidal.snap(cid, (lon, lat))
    if got is None:
        raise HTTPException(404, f"tidal candidate {cid} not found")
    if "error" in got:
        raise HTTPException(422, [{"path": "coord", "msg": got["error"]}])
    return got


@app.get("/api/tidal/candidates/{cid:path}/edge")
def tidal_edge(cid: str):
    """The tidal polygon's edge near one candidate, as lon/lat lines (read only)."""
    import tidal
    got = tidal.edge(cid)
    if got is None:
        raise HTTPException(404, f"tidal candidate {cid} not found")
    return got


@app.get("/api/tidal/candidates/{cid:path}")
def tidal_candidate(cid: str):
    """One proposed tidal boundary: its sources verbatim, the polygon's crossing, the proposed cut,
    labelled map points and the mark in force."""
    import tidal
    got = tidal.detail(cid)
    if got is None:
        raise HTTPException(404, f"tidal candidate {cid} not found")
    return got


# --------------------------------------------------------------------------- T55: Slough review mode
# All READS: the slough / canal candidates and their pre-marks (`pipeline.regs.review.slough`). The
# owner's call is a mark (scope "slough") through POST /api/marks; a pre-mark is never one.

@app.get("/api/sloughs/queue")
def slough_queue(group: str | None = None, basis: str | None = None, verify: str | None = None,
                 q: str | None = None, region: str | None = None):
    """THE SLOUGH QUEUE (`sloughs.queue`): `group` premark | owner list | owner | default, `basis`
    narrow | wide marsh | intermittent, `verify` the mark (todo = unmarked or stale), `q`, `region`."""
    import sloughs
    from pipeline.regs.review.slough import GROUPS
    if group and group not in GROUPS:
        raise HTTPException(422, [{"path": "group", "msg": "group is one of " + ", ".join(GROUPS)}])
    if verify and verify not in sloughs.VERIFIES:
        raise HTTPException(422, [{"path": "verify", "msg": "verify is one of " + ", ".join(sloughs.VERIFIES)}])
    # two candidates have NaN imagery stats (no pixels): JSON has no NaN, so the unfiltered queue 500'd
    return reuse._json_safe(sloughs.queue(group=group, basis=basis, verify=verify, q=q, region=region))


@app.get("/api/sloughs/progress")
def slough_progress():
    import sloughs
    return sloughs.progress()


@app.get("/api/sloughs/candidates/{cid}")
def slough_candidate(cid: str):
    """One slough / canal candidate: its row (pre-mark, why), the finder's evidence, its outline."""
    import sloughs
    got = sloughs.detail(cid)
    if got is None:
        raise HTTPException(404, f"slough candidate {cid} not found")
    return got


# --------------------------------------------------------------------------- Q154: Small ponds (Sloughs tab)
# All READS: the Q154 sample (`pipeline.regs.review.q154`) and the tally of the owner's marks against the
# cutoff's prediction. The owner's call is a mark (scope "q154") through POST /api/marks.

@app.get("/api/q154/queue")
def q154_queue(set: str | None = None, verify: str | None = None, caught: str | None = None):
    """THE Q154 SAMPLE (`ponds.queue`): `set` example | sample, `verify` todo | decided | unsure,
    `caught` yes | no (the cutoff's prediction)."""
    import ponds
    return ponds.queue(set_=set, verify=verify, caught=caught)


@app.get("/api/q154/tally")
def q154_tally():
    import ponds
    return ponds.tally()


@app.get("/api/q154/ponds/{pid}")
def q154_pond(pid: str):
    """One sample pond: its measures, the cutoff's prediction, its outline and the river lines."""
    import ponds
    got = ponds.detail(pid)
    if got is None:
        raise HTTPException(404, f"Q154 sample pond {pid} not found")
    return got


# --------------------------------------------------------------------------- T230: In-season tab
# The provincial in-season feed's rows and where each applies (`inseason.py`). THE ONE WRITE THAT IS NOT A
# MARK (owner 2026-10-28, AGENTS 66): POST /api/inseason/matches edits data/curated/inseason/notice_waters.json —
# Claude/owner tooling, informational only, one history note per edit — through `writes.commit`.

class InSeasonMatchPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str
    #: None / [] clears the match
    extents: list[dict] | None = None
    effective: dict | None = None
    #: REQUIRED: the history note of this edit
    note: str
    by: str = ""


@app.get("/api/inseason/queue")
def inseason_queue():
    """Every in-season feed row (and removed row, and a match on a row the feed no longer holds: "stale") with
    its match state, in-effect flag and history; progress {matched, unmatched, total}. No feed file: an empty
    queue that says so (`available: false`, `message`)."""
    import inseason
    try:
        return inseason.queue()
    except (OSError, ValueError) as ex:
        raise HTTPException(503, f"in-season queue unavailable: {ex}")


@app.get("/api/inseason/items/{item_id}/splits")
def inseason_item_splits(item_id: str):
    """The splits.json splits on one water, as labelled map points — the extent editor's split picker."""
    import inseason
    got = inseason.item_splits(item_id)
    if got is None:
        raise HTTPException(404, f"item {item_id} not found")
    return got


@app.get("/api/inseason/extent-preview")
def inseason_extent_preview(extent: str, limit: int = 6000):
    """What the proposed extents (`extent`: one `{op, item_id, splits}` or a list, as JSON) bind, unioned,
    as geometry — read only."""
    import json as _json
    import inseason
    try:
        exts = inseason.parse_extents(_json.loads(extent))
        return inseason.extent_preview(exts, limit=limit)
    except (ValueError, inseason.Refused) as ex:
        raise HTTPException(422, str(ex))


@app.post("/api/inseason/matches")
def inseason_save_match(body: InSeasonMatchPayload):
    """SET or CLEAR one in-season row's match (`inseason.save_match`): validated (row in the feed, known water
    and splits, the extent binds a section, a note), then `writes.commit` (backup + atomic). 400 when refused, and
    (T434) every refusal logged like a mark's (`REFUSED_LOG`)."""
    import inseason
    try:
        got = inseason.save_match(body.id, body.extents, body.effective, body.note, body.by)
    except inseason.Refused as ex:
        _log_refused(body, 400, str(ex))
        raise HTTPException(400, str(ex))
    # (T520) publish from the Mac: the saved match goes to STAGING by the one Mac path (resolve, publish's refusals,
    # deploy/staging/sync-inputs.sh), in the background; its outcome is GET /api/inputs/sync
    return {**(got or {}), "sync": INPUTS_SYNC.request("inseason-match")}


@app.post("/api/inseason/resolve")
def inseason_resolve_all():
    """(T520) Re-resolve every in-season match on the shipped release (after a release promote), then publish (the Mac
    path): matches that no longer resolve say why and are refused by publish until re-matched."""
    import inseason
    got = inseason.resolve_all()
    return {**got, "sync": INPUTS_SYNC.request("inseason-match")}


@app.get("/api/inputs/sync")
def inputs_sync_status():
    """(T520) The last run of the Mac publish path after a saved input (state queued / running / done / failed, its
    output's tail). A failure is also in the server log and data/generated/review/inputs-sync.jsonl."""
    return INPUTS_SYNC.status()


@app.get("/api/row-image/{filename}")
def row_image(filename: str):
    """Serve a source synopsis row-crop image (<generated>/regs/extraction/row_images/<name>.png)."""
    if not re.fullmatch(r"[A-Za-z0-9_]+\.png", filename):
        raise HTTPException(400, "bad filename")
    path = reuse.ROW_IMAGES_DIR / filename
    if not path.exists():
        raise HTTPException(404, f"{filename} not found")
    return FileResponse(path, media_type="image/png")


@app.get("/api/synopsis.pdf")
def synopsis_pdf():
    """The synopsis PDF the entries were read from (the repo copy), for `#page=<pdf page>` links."""
    import synopsis_pages
    if not synopsis_pages.PDF_PATH.exists():
        raise HTTPException(404, f"{synopsis_pages.PDF_PATH} not found")
    return FileResponse(synopsis_pages.PDF_PATH, media_type="application/pdf")


@app.get("/api/synopsis/pages")
def synopsis_page_map():
    """printed page -> PDF page. `source_pages` are PRINTED numbers; the PDF index differs by 2-6."""
    import synopsis_pages
    return synopsis_pages.printed_to_pdf()


@app.get("/api/synopsis/ref")
def synopsis_ref(printed: int, water: str = ""):
    """Where a printed page (and, when `water` names a row on it, that row) is in the repo PDF: the
    PDF page for `#page=`, and the page rendered with the row boxed / the row's own crop."""
    import evidence
    import synopsis_pages
    n = synopsis_pages.pdf_page(printed)
    if n is None:
        raise HTTPException(404, f"no printed page {printed} in the synopsis")
    out = {"printed": printed, "pdf_page": n, "pdf_url": f"/api/synopsis.pdf#page={n}",
           "page_url": f"/api/synopsis/page/{printed}.png", "row_image": None, "row_page_url": None}
    want = re.sub(r"[^a-z0-9]+", " ", water.lower()).strip()
    if want:
        try:
            rows = evidence.row_boxes()
        except evidence.Missing:
            rows = {}
        hits = [k for k, b in rows.items() if b.get("printed") == printed and
                re.sub(r"[^a-z0-9]+", " ", str(b.get("water", "")).lower()).strip().startswith(want)]
        if hits:
            out["row_image"] = f"/api/row-image/{hits[0]}"
            out["row_page_url"] = f"/api/synopsis/row/{hits[0]}/page.png"
    return out


@app.get("/api/synopsis/row/{key}/page.png")
def synopsis_row_page(key: str):
    """The whole printed page with one source row boxed (the row_boxes.json sidecar's box)."""
    import evidence
    if not re.fullmatch(r"[A-Za-z0-9_]+\.png", key):
        raise HTTPException(400, "bad key")
    try:
        b = evidence.row_boxes().get(key)
    except evidence.Missing as ex:
        raise HTTPException(404, str(ex))
    if b is None:
        raise HTTPException(404, f"{key} not in the row boxes")
    return Response(evidence.page_png({"pdf_page": b["pdf_page"], "boxes": [b["box"]]}),
                    media_type="image/png")


# --------------------------------------------------------------------------- T237: Reports tab
# Problem reports the app sent (the staging feeds worker's private/reports/), read through the worker with the run
# token — READ ONLY: nothing is written here; the reviewer's status per report is kept in the browser (`reports.py`).

@app.get("/api/reports")
def reports_list(since: str | None = None):
    """Every report since `since` (YYYY-MM-DD; default 90 days), newest first, each with the water it names and,
    when its tiles are this build's vintage, the tapped section's node and waters. 503 when the worker cannot be
    asked (no token, offline) — the tab keeps what it synced before."""
    import re as _re
    import reports
    if since is not None and not _re.fullmatch(r"\d{4}-\d{2}-\d{2}", since):
        raise HTTPException(422, "since must be YYYY-MM-DD")
    try:
        got = reports.fetch(since)
    except reports.ReportsUnavailable as ex:
        raise HTTPException(503, str(ex))
    return {"reports": [reports.enrich(r) for r in got], "source": reports._url()}


@app.get("/api/reports/section")
def reports_section(sid: int, tiles: str):
    """The tapped section of a report as map geometry — empty when the report's tiles are another vintage."""
    import reports
    return reports.section_geojson(sid, tiles)


@app.get("/api/synopsis/page/{printed}.png")
def synopsis_page_image(printed: int):
    """The whole printed page, rendered — the region chapter a zone entry was transcribed from has
    no row crop, so this is how its text is checked against the book."""
    import synopsis_pages
    png = synopsis_pages.page_png(printed)
    if png is None:
        raise HTTPException(404, f"no printed page {printed} in the synopsis")
    return Response(png, media_type="image/png")


@app.get("/basemap/bc.pmtiles")
def basemap():
    """Serve the webapp's PMTiles basemap (data/bc.pmtiles). FileResponse honours HTTP Range requests,
    which the pmtiles protocol needs to fetch byte ranges."""
    if not reuse.BASEMAP_PMTILES.exists():
        raise HTTPException(404, f"{reuse.BASEMAP_PMTILES} not found")
    return FileResponse(reuse.BASEMAP_PMTILES, media_type="application/octet-stream")
