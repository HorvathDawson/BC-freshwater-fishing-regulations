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
from pydantic import BaseModel

import rebuild
import reuse

app = FastAPI(title="Curation Review")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


@app.get("/api/regions")
def get_regions():
    out = []
    for r in reuse.regions():
        rows = reuse.queue(region=r)
        out.append({"id": r, "total": len(rows), "by_status": dict(Counter(x["status"] for x in rows))})
    return out


@app.get("/api/entries")
def get_entries(region: str | None = None, status: str | None = None):
    return reuse.queue(region=region, status=status)


@app.get("/api/entries/{entry_id}")
def get_entry(entry_id: str):
    d = reuse.entry_detail(entry_id)
    if d is None:
        raise HTTPException(404, f"entry {entry_id} not found")
    return d


@app.get("/api/items/search")
def search(q: str):
    return reuse.search_items(q)


@app.get("/api/species")
def species():
    """All species/group codes + common names, for the species picker."""
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
    reviewed_by: str = ""


@app.put("/api/entries/{entry_id}")
def save(entry_id: str, body: SavePayload):
    res = reuse.save_entry(body.region, body.entry, lock=False)
    if not res["ok"]:
        raise HTTPException(422, res["errors"])
    return res


@app.post("/api/entries/{entry_id}/confirm")
def confirm(entry_id: str, body: SavePayload):
    res = reuse.save_entry(body.region, body.entry, lock=True, reviewed_by=body.reviewed_by)
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
        raise HTTPException(404, res["errors"])
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
    """Serve a source synopsis row-crop image (output/pipeline/extraction/row_images/<name>.png)."""
    if not re.fullmatch(r"[A-Za-z0-9_]+\.png", filename):
        raise HTTPException(400, "bad filename")
    path = reuse.ROW_IMAGES_DIR / filename
    if not path.exists():
        raise HTTPException(404, f"{filename} not found")
    return FileResponse(path, media_type="image/png")


@app.post("/api/rebuild")
def start_rebuild():
    """Kick off the full graph rebuild (pipeline.build --full) in the background. CPU-only, no credits.
    Bakes splits.json edits into the graph; on success the reuse caches drop so every item refreshes.
    Idempotent while one is running (returns the in-flight status)."""
    return rebuild.MANAGER.start()


@app.get("/api/rebuild/status")
def rebuild_status():
    """Progress of the current/last rebuild: status, elapsed, per-stage checklist, tail of the log."""
    return rebuild.MANAGER.snapshot()


@app.get("/basemap/bc.pmtiles")
def basemap():
    """Serve the webapp's PMTiles basemap (data/bc.pmtiles). FileResponse honours HTTP Range requests,
    which the pmtiles protocol needs to fetch byte ranges."""
    if not reuse.BASEMAP_PMTILES.exists():
        raise HTTPException(404, f"{reuse.BASEMAP_PMTILES} not found")
    return FileResponse(reuse.BASEMAP_PMTILES, media_type="application/octet-stream")
