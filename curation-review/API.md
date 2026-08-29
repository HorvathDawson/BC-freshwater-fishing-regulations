# Curation Review — HTTP API contract

Backend base URL (dev): `http://127.0.0.1:8787`. All responses JSON. No auth. Start with
`bash curation-review/backend/run.sh`. Makes **no LLM calls** — pure local file review, safe to run
out of parsing credits.

**Write model:** `pipeline/parsing/entries/region-*.json` are the SINGLE SOURCE OF TRUTH. Curator
decisions are written straight back to them; the old separate `reviewed/` overlay has been merged in
and retired.

---

## Queue

### GET /api/regions
→ `[{ "id": "1", "total": 211, "by_status": {"no_registry":3,"needs_review":35,"unused_splits":5,"unreviewed":168,"confirmed":0} }]`

### GET /api/entries?region=&status=
Both query params optional. `status` ∈ `no_registry | needs_review | unused_splits | unreviewed | confirmed`.
Rows are pre-sorted (attention first).
```json
{ "entry_id":"gnis:8634", "region":"2", "name":"CHILLIWACK / VEDDER RIVERS", "mus":["2-2"],
  "status":"unreviewed", "locked":false, "revisit":false, "reference_only":false,
  "registry_status":"matched", "n_rules":6,
  "matched_item_id":"gnis:8634", "matched_item_name":"Chilliwack River",
  "also_item_ids":["gnis:3062","wbk:329707189"], "unused_curated_splits":0 }
```
`also_item_ids` — a combined override's OTHER registry items. One synopsis row can regulate several
waters (Chilliwack + Vedder River + Vedder Canal; the Fraser plus twelve side channels), and the entry
covers all of them. `reference_only` marks a "See X" pointer row that carries no regulations itself.

---

## One entry

### GET /api/entries/{entry_id}
```json
{
  "entry": { ...full Entry — see pipeline/parsing/entry_models.py... },
  "region": "2",
  "match": { "item_id":"gnis:8634", "status":"matched|override|skip|ambiguous|no_registry|feature_pin",
             "reason":"", "candidates":[], "also":["gnis:3062","wbk:329707189"] },
  "item": { "id":"gnis:8634", "name":"Chilliwack River", "kind":"stream", "variants":[], "mus":[],
            "boundaries":[ { "id":"chilliwack_vedder_rivers__tamihi_rapids_bridge",
                             "label":"Tamihi Rapids Bridge", "kind":"split",
                             "ref":"split:chilliwack_vedder_rivers__tamihi_rapids_bridge",
                             "wbk":"", "curated":true, "item_id":"gnis:8634",
                             "meta":{"anchor_type":"point","route_measure":11988.68,"blk":"380887781"},
                             "in_graph":true, "in_splits":true, "live":{...} } ] },
  "also_items": [ {"id":"gnis:3062","name":"Vedder River","kind":"stream"} ],
  "related_entries": [ {"entry_id":"wbk:329707189","region":"2","name":"VEDDER RIVER","locked":false,
                        "n_rules":1,"pointer":true,"shared_items":[{"id":"gnis:3062","name":"Vedder River"}]} ],
  "unused_curated_splits": [ {"id":"foo_falls","label":"Foo Falls","anchor_type":"confluence"} ],
  "source_image": "row_00123.png"
}
```
`item.boundaries` is the **union over the item AND `also_items`**, each tagged with its owning
`item_id`, so a combined entry can bind a cut on any water it covers. `related_entries` are other
synopsis rows over the same water — the synopsis splits one regulation across several rows, and
reviewing one without the other is how a half-linked entry gets confirmed.

**Rule shape** (inside `entry.rules[]`):
`rule_id, restriction_type, details, extents[{op,splits[],item_id?,area_id?}], dates[],
includes_tributaries, tributaries_only, tributary_excludes[{op,splits[],item_id?}], sections_override?,
needs_review, review_reason, rule_text, location_text, exception, display_location,
unresolved_locators[], exempts_from[], species[]`

- **`Extent.op`** ∈ `whole | upstream_of | downstream_of | between | within`; `splits` are boundary
  **ids** from `item.boundaries`. `Extent.item_id` scopes the extent to ONE covered item — required on
  a combined entry when a rule applies to only one of its waters.
- **`exempts_from`** — normalized ids of the DEFAULT restrictions this rule lifts. A regional closure
  applies unless a water is exempted from it, so "is this river open?" cannot be answered from the
  closure rules alone; the exemption has to be machine-readable. Vocabulary: `spring_closure`,
  `summer_closure`, `trout_char_release`, `bull_trout_release`, `bait_ban`, `single_barbless_hook`,
  `kokanee_stream_quota`.
- **`tributary_excludes`** — per-rule tributary carve-outs, same shape as `entry.tributaries.excludes`.

### PUT /api/entries/{entry_id}   (save an edit, no lock)
Body: `{ "region":"2", "entry": {...full Entry...} }`
→ `200 {"ok":true,"errors":[]}` · `422 {detail:[...errors...]}` when the entry fails validation
(Entry schema, plus every split id must exist in the boundaries of the item its extent is scoped to).

### POST /api/entries/{entry_id}/confirm   (confirm = lock + stamp)
Body: `{ "region":"2", "entry": {...}, "reviewed_by":"dawson" }`
→ same as PUT, and sets `locked:true`, `reviewed_by`, `reviewed_at` (server timestamp).

### GET /api/entries/{entry_id}/reaches
What each rule actually selects on the map — the answer to "show me what this rule covers".
```json
{ "covered": ["gnis:8634","gnis:3062","wbk:329707189"],
  "rules": { "chilliwack_vedder_rivers.r2": [ { "sections":["380887781:11988", ...],
                                                "unclassified":[],
                                                "ambiguous_cut":[],
                                                "waters":["Chilliwack River"] } ],
             "chilliwack_vedder_rivers.r1": [] } }
```
One array element per extent, in order; `null` for an extent that cannot be resolved to geometry (a
`within(area)` scope, or a cut that is not on the scoped water).

- **`sections`** is exact, and includes braided side channels whose ends both attach inside the reach.
  `upstream_of` and `downstream_of` the same cut never overlap, and together with `unclassified` they
  cover the item exactly.
- **`unclassified`** — pieces that genuinely STRADDLE the reach's end (attached inside on one side and
  outside on the other), surfaced rather than silently included or dropped.
- **`waters`** — the named waters the reach actually lands on, biggest share first. A section id says
  nothing to a reader, and the authored extent text hides the answer: "downstream of Vedder Crossing
  Bridge" does not reveal that it resolves onto the **Vedder**, which is the thing to check.
- **`ambiguous_cut`** — `[{split_id, used, also_at[]}]`. One split id landing at more than one measure
  on the same blue line (an area boundary the stream crosses twice) has two honest readings; the lower
  is used and the alternatives reported, so the curator is told rather than quietly given one of them.

Reaches are computed by ROUTE MEASURE on the cut's own blue line, not by a flow walk. A `between`
whose two cuts sit on DIFFERENT blue lines — a reach spanning a name change, "downstream of Tamihi
Rapids Bridge to Vedder Crossing Bridge" running from the Chilliwack onto the Vedder — is resolved as
the intersection of the two half-lines instead. Such an extent must NOT be scoped with `item_id` to
one of the waters, or the other water's cut falls outside the scope and the reach is unresolvable.

A boundary is looked up by the REF its kind implies (`split:{id}` for a curated cut, `lake:{wbk}` for
a lake outlet), so reaches bounded by a lake resolve like any other.

Reaches come from the built graph, so they reflect the last rebuild — not unsaved edits.

---

## Registry lookups

### GET /api/items/search?q=
→ `[{ "id":"gnis:5480", "name":"Amor De Cosmos Creek", "kind":"stream", "mus":[...] }]` — for attaching
an item to a `no_registry` entry (put the chosen id into `entry.matched`).

### GET /api/items/{item_id}/boundaries
Bindable splits/boundaries for an arbitrary item — the cross-item and tributary-exclude pickers.

### GET /api/items/{item_id}/tributaries
→ `[{ "id":"gnis:1234", "name":"Slesse Creek" }]` — named streams whose mouth flows into this item,
read from the graph's flow edges. The carve-out picker's dropdown.

### GET /api/species
→ `[{ "code":"RB", "name":"Rainbow Trout", "is_group":false }]` — the species picker.

---

## Map

### GET /api/items/{item_id}/geojson
Stream sections + curated split points in EPSG:4326, to overlay the web-mercator PMTiles basemap.
Line features carry `node_id` (matches `reaches[].sections`) and `item_id`; point features carry
`split_id` and `auto`. `properties.kind` is `stream | side_channel | split | waterbody`.

**`side_channel`** — the item's NAMED anabranches, drawn with the river though they are separate
registry items. A named channel is split into its own item on purpose (folded in, its name became the
river's and its rules resolved to the whole mainstem), but that also dropped it off the river's map:
the Fraser drew dozens of anonymous side channels while Seabird Island North Side Channel, Herrling
Island Side Channel and Maria Slough were absent. Told apart from a tributary by direction — a side
channel both takes water from the item and returns it, a tributary only flows in.

### GET /api/items/{item_id}/tributaries/geojson?limit=6000
One level of tributaries as geometry — what `includes_tributaries` actually covers. Each tributary is
drawn in full, not just its joining piece. Their own tributaries are NOT followed.

Returns `n_tributaries`, `n_drawn` and `truncated` alongside the features. `limit` counts SECTIONS,
and only whole tributaries are drawn — a half-drawn stream looks exactly like one that has lost its
connection to the river. At the old cap of 400 the Fraser drew 125 of its 389 tributaries while still
reporting 389, so two thirds were missing with nothing to say so.

### GET /api/row-image/{filename}
The source synopsis row-crop PNG (`output/pipeline/extraction/row_images/`). Filename must match
`[A-Za-z0-9_]+\.png`.

### GET /basemap/bc.pmtiles
Serves `data/bc.pmtiles`, honouring HTTP Range requests (the pmtiles protocol needs byte ranges).

---

## Editing splits.json

These write the hand-curated split source. **A graph rebuild is required for any of them to take
effect** — until then the map still shows the as-built geometry.

### GET /api/splits/{split_id}
→ `{ "split": {...}, "waterbody": "CHILLIWACK RIVER", "applies_to": {...} }` · 404 if not in splits.json.

### PUT /api/splits/{split_id}
Body `{ "patch": {label?, note?, kind?, anchor?} }` → `{ok, errors}`.

### DELETE /api/splits/{split_id}
→ `{ok, errors}` · 404 if absent.

### GET /api/splits/{split_id}/refs
→ `[{entry_id, region, rule_id, details, entry_name}]` — the rules binding this split. The impact
preview before a rename.

### POST /api/splits/{split_id}/rename
Body `{ "new_id": "..." }` → `{ok, new_id, updated_rules[], failed_rules[]}`. Renames the split AND
rewrites every rule that binds it, so a rename never strands a binding.

---

## Rebuild

### POST /api/rebuild
Kicks off `pipeline.build --full --out output/v2/full --splits pipeline/splits.json` as a background
subprocess — CPU-only, **no credits**. Bakes splits.json edits into the section boundaries. Idempotent
while one is running (returns the in-flight status). On success the backend drops its reuse caches —
registry, splits.resolved, gpkg split_points, **and the in-memory graph** — so every subsequent request
serves fresh data and the UI reloads all items.

### GET /api/rebuild/status
`{status: idle|running|done|error, elapsed_s, current, stages:[{label, done, seconds}], n_done, n_total,
returncode, error, log_tail[]}`. `stages` mirrors the pipeline's per-stage `[label: Ns]` timing lines,
in order, for the progress bar. Poll while `running`.

---

### UI must-haves (from the spec)
- Side-by-side **original `regs_verbatim`** ↔ **parsed rules** (each rule's binding shown human + raw).
- The item's **bindable boundaries** menu (curated splits flagged via `boundaries[].curated`), spanning
  every item a combined entry covers.
- **Highlight unused curated splits** (`unused_curated_splits[]`) — a curated cut no rule used.
- **Show reach** per rule, with the straddling / ambiguous-cut warnings surfaced.
- **Related entries** — jump to the other synopsis rows over the same water.
- **Confirm** (POST /confirm), **Save edit** (PUT), and the **attach-item** flow for `no_registry`.
- Queue filterable by region + status, attention-first (already sorted by the API).
