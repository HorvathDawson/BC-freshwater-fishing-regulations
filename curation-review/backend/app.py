"""Curation-review backend — FastAPI over the pipeline-reuse data layer (see reuse.py).

Run it via the helper (sets PYTHONPATH to the repo root so `import pipeline...` works, and runs from
this dir so `import reuse` works — the folder name has a hyphen so it can't be a package):
    bash curation-review/backend/run.sh          # -> http://127.0.0.1:8787

Makes NO LLM calls — pure local file review. Safe to run any time, including out of parsing credits.

REVIEW MODE ONLY (T134, T185): the app serves reads and ONE write, a mark (`POST /api/marks`). A
reviewer cannot change the catalogue or splits.json however they click: the editors and their routes
(entry save, the draft check, split add / move / delete / rename — the old "fix mode") are gone. A
wrong or unsure mark becomes a ticket (`python -m pipeline.regs.review.tickets`); the fix is made in
the curated files by a reviewed change, never by this app (T185).
"""

from __future__ import annotations

import os
import re
from collections import Counter

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, ConfigDict

import reuse

app = FastAPI(title="Curation Review")
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


#: WARM AT STARTUP (T136, PLAN §4 phase 5): `CURATION_WARM=1` (run.sh sets it) loads every cache a
#: first entry reads in a background thread (`reuse.warm`), so the reviewer never waits on a cold
#: backend; `GET /api/ready` says when it is done. Off by default (tests, a one-off script).
WARM = os.environ.get("CURATION_WARM") == "1"


@app.on_event("startup")
def _warm_at_startup():
    if WARM:
        import threading
        threading.Thread(target=reuse.warm, name="curation-warm", daemon=True).start()


@app.get("/api/ready")
def get_ready():
    """THE READY SIGNAL: whether the backend has loaded what a first entry needs (`reuse.warm`),
    step by step with the time each finished — poll it before the reviewer starts."""
    return reuse.readiness()


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
                order: str = "risk", tier: str | None = None):
    """THE ONE QUEUE (T135): every region unless `region` names one. `kind` is zone | water — the
    region chapters or the water tables. `verify` is the reviewer's mark (unmarked | verified |
    wrong | unsure | deferred | stale | todo); `tier` the review tier (PLAN §3); `status` what the
    parse left (`reuse.STATUSES`); `order` is risk (the review order: tier, the most urgent risk
    factor, the severity-A pre-checks, then the book — `tiers.queue_rank`) or book (region, page,
    then the page's order)."""
    if order not in reuse.QUEUE_ORDERS:
        raise HTTPException(422, [{"path": "order",
                                   "msg": "order is " + " or ".join(reuse.QUEUE_ORDERS)}])
    if status and status not in reuse.STATUSES:
        raise HTTPException(422, [{"path": "status",
                                   "msg": "status is one of " + ", ".join(reuse.STATUSES)}])
    return reuse.queue(region=region, status=status, kind=kind, verify=verify, order=order,
                       tier=tier)


#: what a reviewer marks: an entry, or one rule of a zone entry (T184: entries only)
REVIEW_SCOPES = ("entry", "rule")


class MarkPayload(BaseModel):
    """What a reviewer sends; the fingerprints, the date and the book's sha256 are stamped by the
    server (`reuse.post_mark`), never taken from the client."""
    model_config = ConfigDict(extra="forbid")
    scope: str                       # entry | rule (REVIEW_SCOPES)
    target: str                      # entry_id
    rule_id: str | None = None       # scope rule
    #: verified | wrong | unsure | deferred; omitted -> `marks.verdict_from_answers` the answers (T134)
    verdict: str | None = None
    answers: dict[str, str]          # T / R / P / A -> y | n | ?
    problem: str | None = None
    note: str = ""
    reviewer: str
    pages: list[int] = []            # printed pages read (default: the entry's source_pages)
    #: the reviewer's time on the target before this mark, by the screen's clock (T136 pilot)
    seconds: float | None = None


@app.post("/api/marks")
def post_mark(body: MarkPayload):
    """Record a reviewer's mark (T97) in the append-only store `marks.jsonl`, against the
    target's fingerprints NOW. THE ONE WRITE OF REVIEW MODE. Refused (422) on an entry that fails
    the gate, or a mark the model refuses (a verdict its answers do not support, no reviewer);
    404 for an unknown target. With no `verdict` the answers decide it (`marks.verdict_from_answers`)."""
    from pydantic import ValidationError
    from pipeline.regs.review.marks import (CHECKS, refusal_words, unanswered_words,
                                            verdict_from_answers)
    data = body.model_dump()
    if body.scope not in REVIEW_SCOPES:
        raise HTTPException(422, [{"path": "scope", "msg": (
            "A split is not marked on its own: check it on the map of an entry whose rules name "
            "it, and mark that entry's Place wrong if the cut is in the wrong place."
            if body.scope == "split" else f"scope is one of {list(REVIEW_SCOPES)}")}])
    # A REFUSAL IS SAID IN THE REVIEWER'S WORDS (T142, `marks.refusal_words`): what to do, never
    # the model's "Value error, …" — and with no field path, which means nothing at the screen.
    if data["verdict"] is None:
        data["verdict"] = verdict_from_answers(body.scope, body.answers)
        if data["verdict"] is None:
            raise HTTPException(422, [{"path": "", "msg": unanswered_words(
                body.scope, awaiting=reuse.awaiting_build(body.scope, body.target))}])
    try:
        return reuse.post_mark(data)
    except KeyError as ex:
        raise HTTPException(404, f"{body.scope} {body.target}"
                                 f"{'/' + body.rule_id if body.rule_id else ''} not found: {ex}")
    except ValidationError as ex:
        raise HTTPException(422, [{"path": "", "msg": refusal_words(
            e["msg"], str(e["loc"][0]) if e["loc"] else None)} for e in ex.errors()])
    except ValueError as ex:
        raise HTTPException(422, [{"path": "", "msg": refusal_words(str(ex))}])


@app.get("/api/marks")
def get_marks(target: str | None = None, scope: str | None = None):
    """The marks in force (the last one on each target), each with its `status` and typed stale
    `reasons` against its target as it is now."""
    return reuse.marks_view(target, scope)


@app.get("/api/marks/splits")
def get_split_marks():
    """Every split in splits.json -> its old split mark's status now (`unmarked` for most). Split
    marks are HISTORY (T184): none is taken any more, the ones in the store stay readable."""
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


@app.get("/api/review/tickets")
def review_tickets():
    """THE TICKET EXPORT (T135): every wrong and unsure mark in force as ticket text, with what
    changed since. Read only — the owner files them (`python -m pipeline.regs.review.tickets
    --out <dir>` writes the same text to files)."""
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
    match. For an entry AWAITING BUILD only its `vintage` (the bundle's copy is not this entry's)."""
    d = reuse.entry_answer_context(entry_id)
    if d is None:
        raise HTTPException(404, f"entry {entry_id} not found")
    return d


@app.get("/api/entries/{entry_id}/answer/waters")
def entry_answer_waters(entry_id: str, q: str = "", limit: int = 60):
    """The waters this entry reaches in the bundle, split into pieces that get one answer each.
    409 while the entry awaits its build (T133): the bundle's answer is another vintage's."""
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
    one fish — plus the licensing bound there and whether it is tidal. Reflects the live bundle."""
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
    return d


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
