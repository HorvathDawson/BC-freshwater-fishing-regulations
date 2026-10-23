"""Curation-review backend — FastAPI over the pipeline-reuse data layer (see reuse.py).

Run it via the helper (sets PYTHONPATH to the repo root so `import pipeline...` works, and runs from
this dir so `import reuse` works — the folder name has a hyphen so it can't be a package):
    bash curation-review/backend/run.sh          # -> http://127.0.0.1:8787

Makes NO LLM calls — pure local file review. Safe to run any time, including out of parsing credits.
"""

from __future__ import annotations

import re
from collections import Counter

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict

import reuse

app = FastAPI(title="Curation Review")
#: THE ONLY ORIGIN THE BROWSER MAY CALL FROM: the Vite dev server, which binds 127.0.0.1 only
#: (`frontend/vite.config.ts`). It proxies `/api`, so the app itself never needs CORS; this is what
#: stops another page the reviewer has open from reading or writing the catalogue (T96: it was `*`).
VITE_ORIGIN = "http://127.0.0.1:5173"
app.add_middleware(
    CORSMiddleware, allow_origins=[VITE_ORIGIN], allow_methods=["GET", "PUT", "POST", "DELETE"],
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


@app.get("/api/regions")
def get_regions():
    out = []
    for r in reuse.regions():
        rows = reuse.queue(region=r)
        out.append({"id": r, "total": len(rows), "by_status": dict(Counter(x["status"] for x in rows)),
                    "by_verify": dict(Counter(x["verify"] for x in rows))})
    return out


@app.get("/api/entries")
def get_entries(region: str | None = None, status: str | None = None,
                kind: str | None = None, verify: str | None = None,
                order: str = "attention"):
    """`kind` is zone | water — the region chapters or the water tables. They are two jobs.
    `verify` is the reviewer's mark (unverified | verified | stale | flagged | todo); `order` is
    attention (what needs a look first) or book (region, page, then the page's order)."""
    if order not in ("attention", "book"):
        raise HTTPException(422, [{"path": "order", "msg": "order is attention or book"}])
    return reuse.queue(region=region, status=status, kind=kind, verify=verify, order=order)


class MarkPayload(BaseModel):
    """What a reviewer sends; the fingerprints, the date and the book's sha256 are stamped by the
    server (`reuse.post_mark`), never taken from the client."""
    model_config = ConfigDict(extra="forbid")
    scope: str                       # entry | rule | split
    target: str                      # entry_id, or a split id
    rule_id: str | None = None       # scope rule
    verdict: str                     # verified | wrong | unsure | deferred
    answers: dict[str, str]          # T / R / P / A -> y | n | ?
    problem: str | None = None
    note: str = ""
    reviewer: str
    pages: list[int] = []            # printed pages read (default: the entry's source_pages)


@app.post("/api/marks")
def post_mark(body: MarkPayload):
    """Record a reviewer's mark (T97) in the append-only store `marks.jsonl`, against the
    target's fingerprints NOW. Refused (422) on an entry that fails the gate, or a mark the model
    refuses (a verdict its answers do not support, no reviewer); 404 for an unknown target."""
    from pydantic import ValidationError
    try:
        return reuse.post_mark(body.model_dump())
    except KeyError as ex:
        raise HTTPException(404, f"{body.scope} {body.target}"
                                 f"{'/' + body.rule_id if body.rule_id else ''} not found: {ex}")
    except ValidationError as ex:
        raise HTTPException(422, [{"path": ".".join(str(x) for x in e["loc"]), "msg": e["msg"]}
                                  for e in ex.errors()])
    except ValueError as ex:
        raise HTTPException(422, [{"path": "", "msg": str(ex)}])


@app.get("/api/marks")
def get_marks(target: str | None = None, scope: str | None = None):
    """The marks in force (the last one on each target), each with its `status` and typed stale
    `reasons` against its target as it is now."""
    return reuse.marks_view(target, scope)


@app.get("/api/marks/splits")
def get_split_marks():
    """Every split in splits.json -> its mark status now; a split added since the last review
    reads `unmarked`, queued for its own `split` mark."""
    return reuse.split_mark_states()


@app.get("/api/build")
def pending_build():
    """THE PENDING BUILD (T133, PLAN §5): every entry and split awaiting its build, why, the
    vintage on disk, and the command(s) the human runs — the tool never runs them. Re-read from
    disk on every request, so it empties when the rebuild lands, with no restart."""
    return reuse.pending_build()


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


@app.get("/api/vocab")
def get_vocab():
    """Every option list the editors offer — rule types, slots, conduct acts, documents, Who axes,
    species, … — READ OFF the catalogue model (`model_api.vocab`). Served rather than hardcoded in
    the UI: the rule-type picker once listed six retired kinds, every one of which the model
    refused."""
    return reuse.vocab()


@app.get("/api/items/search")
def search(q: str):
    return reuse.search_items(q)


@app.get("/api/species")
def species():
    """Every species code a rule may name (`catalogue.KNOWN_SPECIES`), with the model's words."""
    return reuse.species_list()


@app.get("/api/items/{item_id}/boundaries")
def item_boundaries(item_id: str):
    """Bindable splits/boundaries for an arbitrary item — for tributary-exclude / cross-item pickers."""
    return reuse.item_bindable(item_id)


@app.get("/api/items/{item_id}/tributaries")
def item_tributaries(item_id: str):
    """Named tributaries of an item — the carve-out picker's dropdown."""
    return reuse.item_tributaries(item_id)


class SavePayload(BaseModel):
    region: str
    entry: dict


class CheckPayload(BaseModel):
    region: str | None = None
    entry: dict


@app.post("/api/check")
def check(body: CheckPayload):
    """Validate a DRAFT without writing it: `{ok, errors, warnings, labels}`. `errors` are what a
    save would refuse, each `{path, msg}` addressed to the field (`rules.2.gear.0.max`);
    `labels` is the generated label of every rule and licensing record that validates on its own,
    so the editor shows what the app will say while the curator types."""
    return reuse.check_entry(body.entry, body.region)


@app.put("/api/entries/{entry_id}")
def save(entry_id: str, body: SavePayload):
    if body.entry.get("entry_id") != entry_id:
        raise HTTPException(422, [{"path": "entry_id",
                                   "msg": f"the body is entry {body.entry.get('entry_id')!r}, "
                                          f"the URL is {entry_id!r}"}])
    res = reuse.save_entry(body.region, body.entry)
    if not res["ok"]:
        raise HTTPException(422, res["errors"])
    return res


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


@app.get("/api/splits/{split_id}")
def get_split(split_id: str):
    d = reuse.get_split(split_id)
    if d is None:
        raise HTTPException(404, f"split {split_id} not in splits.json")
    return d


class SplitPatch(BaseModel):
    patch: dict


@app.put("/api/splits/{split_id}")
def save_split(split_id: str, body: SplitPatch):
    """Edit a split in splits.json (label/note/kind/anchor). Needs a graph rebuild to take effect."""
    res = reuse.save_split(split_id, body.patch)
    if not res["ok"]:
        raise HTTPException(422, res["errors"])
    return res


@app.delete("/api/splits/{split_id}")
def delete_split(split_id: str):
    res = reuse.delete_split(split_id)
    if not res["ok"]:
        raise HTTPException(res.get("status", 404), res["errors"])
    return res


@app.get("/api/splits/{split_id}/refs")
def split_refs(split_id: str):
    """Rules that bind this split id — the impact preview before a rename."""
    return reuse.split_refs(split_id)


class RenamePayload(BaseModel):
    new_id: str


@app.post("/api/splits/{split_id}/rename")
def rename_split(split_id: str, body: RenamePayload):
    """Rename a split id + rewrite every rule that binds it. Rebuild to reconcile the graph."""
    res = reuse.rename_split(split_id, body.new_id)
    if not res["ok"]:
        raise HTTPException(422, res["errors"])
    return res


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
    from fastapi.responses import Response
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
