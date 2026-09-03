# Curation Review — build scope & subagent plan

Scoping for building the app in [README.md](README.md) as a **parallel subagent build**. MVP first
(text + splits + confirm + no-registry attach); map and per-section colouring follow.

> **Cost note:** building this spends Claude/subagent tokens (my session), **not** your parsing credits.
> The finished app makes **no LLM calls** — it reads/writes local files — so you can run it for free while
> waiting on parsing credits. That's the whole point.

---

## Module breakdown

| # | Module | What | Size | Depends on |
|---|--------|------|------|-----------|
| 0 | **Contract + scaffold** | `curation-review/{backend,frontend}` skeleton, the API contract (request/response shapes), `Entry` model change (`reviewed_by`/`reviewed_at`), tiny fixture (1–2 trimmed region files + a trimmed registry) so agents build without the 15 MB registry / real data | S | — |
| 1 | **Backend: data + queue** | FastAPI app; load entries + registry + splits; `GET /regions`, `/entries` (queue w/ status+lock), `/entries/{id}` (entry + matched item boundaries/variants + reviewer findings), `/items/search` | M | 0 |
| 2 | **Backend: write + validate** | `PUT /entries/{id}` and `POST /entries/{id}/lock` → validate via `entry_models.Entry` + `validate_entry_splits`, atomic write, stamp `reviewed_by`/`at` | S–M | 0 |
| 3 | **Backend: geojson** | `GET /items/{id}/geojson` → query `graph.gpkg` (`streams`, `split_points`) by blk/wsc, **reproject 3005→4326**, return FeatureCollection | M | 0 |
| 4 | **Frontend: shell + review** | Vite skeleton; filter bar + queue list + detail pane; reg-text ↔ parsed-rules side-by-side; per-rule binding shown human + raw; the split menu; API client | L | 0 (mock), 1 (live) |
| 5 | **Frontend: edit + confirm** | extent editor (op + split dropdowns), species picker, `matched` search+attach (no-registry flow), **Confirm/lock** button; optimistic save → PUT | M | 4, 2 |
| 6 | **Frontend: map** | MapLibre GL panel; load geojson; highlight referenced split(s) + up/down/between direction; OSM raster basemap | M | 4, 3 |
| 7 | **Resolver + colouring** (Phase 2) | light resolver (split→bounding sections, op→section set by route measure) → colour the sections each rule covers | M | 3, 6 |

Rough total: ~1.5–2k LOC (backend ~600, frontend ~1k, resolver ~200). MVP = modules 0–5 (skip 6–7).

---

## Build waves (dependency order)

```
Wave 0  (serial, me)     ── module 0: contract + scaffold + model change + fixture
                              │  everything downstream builds against the frozen contract
Wave 1  (parallel ×3)    ──┬─ agent BE   : modules 1 + 2  (backend data/queue/search + write/validate)
                           ├─ agent GEO  : module 3        (gpkg → geojson)
                           └─ agent FE   : modules 4 + 5   (frontend shell/review/edit, vs mock then live)
Wave 2  (me + 1 agent)   ──┬─ integration: wire FE↔BE against REAL data, fix seams, end-to-end smoke
                           └─ agent MAP  : module 6        (map panel vs GEO's contract)
Wave 3  (optional)       ──── agent RES  : module 7        (resolver + per-section colouring)
```

**Why this split works:** the frontend and backend meet only at the HTTP contract (frozen in Wave 0),
so BE / GEO / FE build in parallel against it. The map (6) is a self-contained component with one input
(the geojson endpoint), so it slots in after the shell exists. Colouring (7) is deferred because it
needs the resolver.

**Subagents needed:** ~4 for MVP (BE, GEO, FE, + integration is me), +1 for map, +1 for resolver.

---

## Decisions to lock in Wave 0 (so agents don't diverge)

1. **Stack:** FastAPI + `uvicorn`; frontend Vite + vanilla TS (or minimal React) + MapLibre GL. One
   `curation-review/` top-level dir, its own `requirements.txt` / `package.json` (not tied to `webapp/`).
2. **API contract:** exact JSON shapes for entry/queue/geojson/search + the PUT payload. Written as a
   short `API.md` + Pydantic response models the backend and a TS types file share by hand.
3. **Entry model change:** add `reviewed_by: str = ""`, `reviewed_at: str = ""` to
   `pipeline/regs/parsing/entry_models.py::Entry` (+ ingest already preserves them via `locked`). One tiny
   test.
4. **Fixture data:** trimmed `region-1.json` (≈5 entries incl. one no-registry + one needs_review) and a
   trimmed registry + a small gpkg slice, so agents run without the big artifacts.
5. **CRS:** geojson served in EPSG:4326 (reproject from 3005). Basemap = OSM raster (needs internet, no
   key; fine — being out of *parsing credits* doesn't mean offline).

## Risks / unknowns

- **`graph.gpkg` query shape** — confirm a stream is selectable by the registry item's `blk`/`wsc` with a
  fast `where=` (avoid full-layer reads). GEO agent validates this first.
- **Per-section colouring needs the resolver** (not built). MVP ships splits-only; colouring is Wave 3.
  Decide: build the real resolver in `pipeline/` (reusable for the product) vs the light in-tool one.
- **Frontend framework** — vanilla keeps it tiny; React is comfier for the edit forms. Pick in Wave 0.

## Recommendation

Do **Wave 0 myself** (contract + scaffold + model change + fixtures — the piece that makes parallelism
safe), then spawn **3 agents for Wave 1**. MVP (waves 0–2) is the usable review tool; map + colouring are
follow-ons. Building MVP is a handful of subagent tasks, not a big project — the data model and
validators already exist, so most of the work is plumbing + UI.
