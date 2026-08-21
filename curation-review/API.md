# Curation Review — HTTP API contract

Backend base URL (dev): `http://127.0.0.1:8787`. All responses JSON. No auth. Start with
`bash curation-review/backend/run.sh`.

## GET /api/regions
→ `[{ "id": "1", "total": 211, "by_status": {"no_registry":3,"needs_review":35,"unused_splits":5,"unreviewed":168,"confirmed":0} } ]`

## GET /api/entries?region=&status=
Both query params optional. `status` ∈ `no_registry | needs_review | unused_splits | unreviewed | confirmed`.
Rows are pre-sorted (attention first). Row:
```json
{ "entry_id":"noreg_link_river_106", "region":"1", "name":"\"LINK\" RIVER", "mus":["1-9"],
  "status":"no_registry", "locked":false, "registry_status":"no_registry",
  "n_rules":2, "matched_item_id":null, "matched_item_name":null, "unused_curated_splits":0 }
```

## GET /api/entries/{entry_id}
```json
{
  "entry": { ...full Entry (see pipeline/parsing/entry_models.py): entry_id, identity{name,region,mus},
             regs_verbatim, registry_status, registry_note, locked, reviewed_by, reviewed_at,
             matched[], tributaries{included,only,excludes}, scope[], rules[...], audit_log[] },
  "region": "1",
  "match": { "item_id":"gnis:5480"|null, "status":"matched|override|skip|ambiguous|no_registry|feature_pin",
             "reason":"", "candidates":[] },
  "item": null | { "id":"gnis:5480", "name":"Amor De Cosmos Creek", "kind":"stream",
                   "variants":[...], "mus":[...],
                   "boundaries":[ {"id":"foo_falls","label":"Foo Falls","kind":"confluence",
                                   "ref":"split:foo_falls","wbk":"","curated":true}, ... ] },
  "unused_curated_splits": [ {"id":"foo_falls","label":"Foo Falls","anchor_type":"confluence"} ]
}
```
**Rule shape** (inside `entry.rules[]`): `rule_id, restriction_type, details, extents[{op,splits[],item?,area?}],
dates[], includes_tributaries, tributary_excludes[{op,splits[],item?}], sections_override?, needs_review, review_reason, rule_text, location_text,
exception, display_location, unresolved_locators[], species[]`. **Extent.op** ∈
`whole|upstream_of|downstream_of|between|within`; `splits` are boundary **ids** from `item.boundaries`.
`tributary_excludes` = per-rule tributary carve-outs (same shape as `entry.tributaries.excludes`, scoped to one rule).

## GET /api/items/search?q=
→ `[{ "id":"gnis:5480", "name":"Amor De Cosmos Creek", "kind":"stream", "mus":[...] }]` (for attaching an
item to a `no_registry` entry — put the chosen id into `entry.matched`).

## PUT /api/entries/{entry_id}   (save an edit, no lock)
Body: `{ "region":"1", "entry": {...full Entry...} }`
→ `200 {"ok":true,"errors":[]}` · `422 {detail:[...errors...]}` if the entry fails validation
(Entry schema + split ids must exist in the matched item's boundaries). Writes to the **reviewed overlay**
`pipeline/parsing/entries/reviewed/region-N.json` — never the parser's file.

## POST /api/entries/{entry_id}/confirm   (confirm = lock + stamp)
Body: `{ "region":"1", "entry": {...}, "reviewed_by":"dawson" }`
→ same as PUT but sets `locked:true`, `reviewed_by`, `reviewed_at` (server timestamp). Validated + written
to the reviewed overlay.

## GET /api/items/{item_id}/geojson   (map — STUBBED)
Currently returns an empty FeatureCollection with `_todo`. Wire the map panel to it; real geometry
(streams + split_points from graph.gpkg, reprojected to 4326) lands with the GEO task.

## POST /api/rebuild   (rebuild the graph)
Kicks off `pipeline.build --full --out output/v2/full --splits pipeline/splits.json` as a background
subprocess — CPU-only, **no credits**. Bakes splits.json edits into the section boundaries. Idempotent
while one is running (returns the in-flight status). Returns the same shape as the status endpoint. On a
successful build the backend drops its reuse caches, so every subsequent request serves the fresh
registry / splits.resolved / gpkg (the UI reloads all items automatically).

## GET /api/rebuild/status
`{status: idle|running|done|error, elapsed_s, current, stages:[{label, done, seconds}], n_done, n_total,
returncode, error, log_tail[]}`. `stages` mirrors the pipeline's per-stage `[label: Ns]` timing lines,
in order, for the progress bar. Poll while `running`.

---

### UI must-haves (from the spec)
- Side-by-side **original `regs_verbatim`** ↔ **parsed rules** (with each rule's binding shown human +
  raw op/splits).
- The item's **bindable boundaries** menu (curated splits flagged via `boundaries[].curated`).
- **Highlight unused curated splits** (`unused_curated_splits[]`) — a curated cut no rule used.
- **Confirm** (POST /confirm), **Save edit** (PUT), and the **attach-item** flow for `no_registry`
  (search → set `entry.matched`).
- Queue filterable by region + status, attention-first (already sorted by the API).
