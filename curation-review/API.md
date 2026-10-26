# Curation Review — HTTP API contract

Backend base URL (dev): `http://127.0.0.1:8787`. All responses JSON. No auth. Start with
`bash curation-review/backend/run.sh`. Makes **no LLM calls** — pure local file review, safe to run
out of parsing credits.

**Write model: ONE write, a mark** (T134, T185). The app reads the catalogue region files
(`data/curated/regulations/entries/catalogue/region-*.json`, the single source of truth) and
splits.json, and appends reviewers' marks to the marks store — `POST /api/marks` is its only write
route. The editors' routes (`POST /api/check`, `PUT /api/entries/{id}`, split add / save / delete /
rename — the retired fix mode) are gone: a request to one is refused by the router (404 / 405). A
fix is a reviewed change to the curated files (T185), never this app's.
`GET /api/mode` → `{app_url, checks: {entry, rule}, problems: {T, R, P, A: [..]},
prechecks: {code: {severity, what}}, commands: {commit_marks, tickets, prechecks}}` (T135: the
commands the screen PRINTS for the human; it runs none).

**Entries only (T184).** No split is a review target: the queue, and the tiers are
entries; a split is checked BY PROXY on the map of each entry whose rules name it
(`GET /api/entries/{id}/splits`), and a wrong one is a "Place: wrong" mark on such an entry.

---

## Queue

### GET /api/regions
→ `[{ "id": "1", "total": 211, "by_status": {"no_registry":3,"parser-uncertain":35,"unused_splits":5,"plain":168,"zone":0} }]`

### GET /api/entries?region=&status=
Both query params optional; **no `region` is THE ONE QUEUE over every region** (T135). `status` ∈
`no_registry | parser-uncertain | unused_splits | plain | zone` (`422` otherwise) — what the PARSE and
the build left, never review progress (that is `verify`). `no_registry` = a water row whose `matched`
names no item of this build (it binds nothing); `parser-uncertain` (was "flagged") = a rule or licensing
record carries a `review_reason` (or a rule an unresolved locator); `plain` (was "unreviewed") =
nothing the parser or the build flagged.
Rows are pre-sorted in the review order (`order=risk`, below).
```json
{ "entry_id":"r2:chilliwack_vedder_rivers@2-2", "region":"2", "name":"CHILLIWACK / VEDDER RIVERS",
  "mus":["2-2"], "status":"plain", "kind":"water", "n_rules":6,
  "matched_item_id":"gnis:8634", "matched_item_name":"Chilliwack River",
  "also_item_ids":["gnis:3062","wbk:329707189"], "unused_curated_splits":0 }
```
`also_item_ids` — a combined override's OTHER registry items. One synopsis row can regulate several
waters (Chilliwack + Vedder River + Vedder Canal; the Fraser plus twelve side channels), and the entry
covers all of them.

---

## One entry

### GET /api/entries/{entry_id}
```json
{
  "entry": { ...a CatalogueEntry — see pipeline/regs/parsing/catalogue.py; each rule AND each
             licensing record also carries its generated `label` (catalogue.label /
             catalogue.licensing_label), which is not stored... },
  "region": "2",
  "item": { "id":"gnis:8634", "name":"Chilliwack River", "kind":"stream", "variants":[], "mus":[],
            "boundaries":[ { "id":"chilliwack_vedder_rivers__tamihi_rapids_bridge",
                             "label":"Tamihi Rapids Bridge", "kind":"split",
                             "ref":"split:chilliwack_vedder_rivers__tamihi_rapids_bridge",
                             "wbk":"", "curated":true, "item_id":"gnis:8634",
                             "meta":{"anchor_type":"point","route_measure":11988.68,"blk":"380887781"},
                             "in_graph":true, "in_splits":true, "live":{...} } ] },
  "also_items": [ {"id":"gnis:3062","name":"Vedder River","kind":"stream"} ],
  "related_entries": [ {"entry_id":"r2:vedder_river@2-2","region":"2","name":"VEDDER RIVER",
                        "n_rules":1,"pointer":true,"shared_items":[{"id":"gnis:3062","name":"Vedder River"}]} ],
  "unused_curated_splits": [ {"id":"foo_falls","label":"Foo Falls","anchor_type":"confluence"} ],
  "source_image": "row_00123.png",  // matched from the synopsis rows by regs_verbatim
  "marks": [...], "vintage": {...},   // the marks in force; one vintage on screen (below)
  "reviewed": {"by": ["Ann"], "on": "2026-10-24", "holds": true}   // or null (T184, below)
}
```
`item.boundaries` is the **union over the item AND `also_items`**, each tagged with its owning
`item_id`, so a combined entry can bind a cut on any water it covers. `related_entries` are other
synopsis rows over the same water — the synopsis splits one regulation across several rows, and
reviewing one without the other is how a half-linked entry gets signed off.

**`matched` is authoritative.** The items an entry covers are `entry.matched` (filtered to this
build's registry, via `pipeline.atlas.reach.covered.covered_ids` with no matcher). An empty
`matched` binds nothing; there is no live re-match fallback.

**The entry is the model, whole.** `entry.rules[]` are `CatalogueRule`s and `entry.licensing[]` are
`LicensingRecord`s (`kind` ∈ designation | not_classified | requirement | licence_terms | exemption |
alternative); `pipeline/regs/parsing/catalogue.py` is the definition. The app shows it and edits none
of it; the pass-through fields (`entry_id`, `name`, `display_name`, `region`, `regs_verbatim`,
`source_pages`, `symbols`) come from the synopsis row.

- **`Extent.op`** ∈ `whole | upstream_of | downstream_of | between | within` (from `entry_models.Op`);
  `splits` are boundary **ids** from `item.boundaries`. `Extent.item_id` scopes the extent to ONE
  covered item. Also `item_ids`, `area_id`, `area_kind`, `feature_types`, `within_area`,
  `outside_area`, `outside_areas` — the keys of `entry_models.Extent`.
- **`tributary_excludes`** — per-rule / per-designation tributary carve-outs, extents of the same shape.

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

Reaches come from the built graph and the entry as it is in its file (a change to the file is
reflected on the next call). `verdict` is the builder's answer per rule
(`outcome`, `reason`, `n_sections`, `sections`, `diagnostics`) — what ships; the UI binds and
highlights from it, and falls back to the raw per-extent resolve only when it is absent.

---

## The review pass

### GET /api/entries?…&verify=&order=
`verify` ∈ `unmarked | verified | wrong | unsure | deferred | stale | todo | recheck` (todo = not
currently verified — human-reviewed; recheck = human-reviewed once and changed since, T184); `order` ∈ `risk` (default) `| book` (region, printed page, chapter before tables, file
order); anything else `422`. **`risk` is the review order (T135, PLAN §3):** by tier, within a tier the
most urgent risk factor (`tiers.RISKS`: no match, unresolved, undrawn part, ambiguous cut, cut, several
waters, flag), then the more severity-A pre-checks, then the book's order (`tiers.queue_rank`).
Entries only (T184). Each row also carries `page` (first printed page), `pointer` (a "See X" row with
no rules), `verify`, `verify_note`, `mark_reasons` (the stale reasons) and `reviewed` (below). `GET /api/regions` adds
`by_verify` counts.

`tier` filters on the review tier (`P1 | Z | P2 | P3 | P4 | sweep`,
`pipeline.regs.review.tiers.entry_tier`); each row carries `tier`, `risks` (most urgent first) and
`prechecks: {A, B}` (how many findings of each severity, `precheck.prechecks`). The `unresolved` and
`ambiguous cut` factors come from the served reach run (`runfacts.RunFacts`: `rule_diagnostic` /
`rule_unresolved`, read only when `report.json` says the run is complete); with no run they are not
claimed and `GET /api/review/progress` `facts` says why.

**Severity (owner 2026-10-26).** Each row (and `GET /api/entries/{id}/review`) carries `severity`
(`high | medium | low | none`, `pipeline.regs.review.severity.entry_severity`) and `severity_reasons`
(most severe first — the chip's hover). The `risk` order puts severity FIRST, then `tiers.queue_rank`.
`severity` filters on it: a level, or `issues` (high + medium); anything else `422`. High: parser
flag, missing split (a named undrawn part, an unresolved locator), unresolved, unplaced, ambiguous
cut, binds nothing, failed gate, unused curated cut. Medium: split to verify, quote not in row.
Low: cuts its water, several waters, a cut decided by hand, unquoted text, an "on parts" note.

### GET /api/review/tickets  ·  GET /api/review/tickets.md  (T135)
Every **wrong** and **unsure** mark in force as ticket text (`pipeline.regs.review.tickets`):
`[{name, text}]`, or one Markdown text. Read only — the owner files them (PLAN §6 Q3). A ticket carries
the mark's own fields (scope, target, rule, verdict, answers, problem, the reviewer's note, reviewer,
date, printed pages, the book's sha256), the entry or split it is about, what changed since (when the
mark went stale) and the work: a reviewed change to the curated files (T185), then the rebuild
that ships it. `tickets.parse_ticket` reads
the mark's fields back (the round trip is a test). CLI: `python -m pipeline.regs.review.tickets --out <dir>`
— the same text, "what changed since" included (`tickets.statuses_now` for both); two marks whose
targets slug to one file name are refused, never written over each other.

### Commit marks (T135)
`python -m pipeline.regs.review.commit_marks [-m msg] [--dry-run]` — `git commit --only` of
`data/curated/regulations/review/marks.jsonl` and nothing else (staged and edited files elsewhere stay
as they were). It commits whenever the store differs from HEAD, a store that lost lines included (a
migration's rewrite adds none). Not an endpoint: review mode writes one thing, a mark; the screen prints
the command.

### POST /api/marks  (T97)
Body `{scope: entry | rule, target, rule_id?, verdict?: verified | wrong | unsure | deferred,
answers: {T, R, P, A: y | n | ?}, problem?, note, reviewer, pages?, seconds?, tributaries?,
lake_split_later?}`. **T189:** `tributaries` `{"entry" | rule_id: include | exclude}` is the
reviewer's call (a rule mark calls only its own rule); `lake_split_later` puts the target on the
deferred lake-split list (`GET /api/review/lake-splits`, `marks.lake_splits_later`; CLI
`python -m pipeline.regs.review.tickets --lake-splits`). Both live in the mark — never a file edit.
**T191:** `extent_fix?` `[{scope: "entry" | rule_id, op, points: [{kind: split | confluence |
lake_inlet | lake_outlet | coord, split_id?, name?, item_id?, coord?: [lon, lat]}], items: [registry
id], tributaries?: bool | null, area?}]` — the reviewer's proposed extent (`marks.ExtentFix`): `op` is
the catalogue's own (`marks.OP_POINTS`: whole 0, upstream_of 1, downstream_of 1, between 2, within /
rest / steelhead_waters 0), one fix per scope, a rule mark fixes only its rule, only on a wrong or
unsure mark. In the ticket as `- **Extent fix:**` JSON (`parse_ticket` reads it back) for T185.
Its reads: `GET /api/extent-fix/ops` (ops, point counts, the owner's words), `GET
/api/splits/search?q=` (curated splits by id / label / water), `GET /api/items/search?q=&kind=`
(registry items by name), `GET /api/entries/{id}/extent-preview?fix=<ExtentFix JSON>` →
`{resolvable, why, n_sections, outcomes, truncated, geojson}` — `build_reach` on an in-memory copy;
a NEW point is `resolvable: false` until T185 builds it. **T184:** scope
`split` is refused (`422`, "A split is not marked on its own: check it on the map of an entry whose
rules name it, and mark that entry's Place wrong…").
**T134:** with no `verdict` the answers decide it (`marks.verdict_from_answers`: any n → wrong, else
any ? → unsure, all y → verified; `422` while a check is unanswered); `deferred` is only ever sent.
**T142:** a refusal is said in the reviewer's words (`marks.refusal_words` / `unanswered_words`), with
`path: ""` — e.g. "A wrong mark needs a problem (pick one from the list under the checklist) and a note
saying what is wrong.", never pydantic's "Value error, …". A target awaiting its build is told that only
T and R can be answered and "all fine" waits for the build (wrong / unsure / defer now), not to "answer
every check" (`unanswered_words(scope, awaiting=reuse.awaiting_build(...))`).
The server stamps `date`, `pdf_sha256` (the repo PDF) and the target's fingerprints
(`pipeline.regs.review.fingerprint`), and APPENDS the mark to `data/curated/regulations/review/marks.jsonl`
(or `CURATION_MARKS`) through the backed-up commit. `422` for a mark the model refuses (`verified`
needs every check `y`; `wrong` a `n`, a problem and a note; no reviewer; an unknown field) or an
entry that FAILS THE GATE; `404` for an unknown entry or rule. → the mark with `status` and
`reasons`. **T133:** also `422` ("awaiting build …", with the command to run) when the target is
AWAITING ITS BUILD and the mark answers anything but `T` and `R` — so no `verified` mark until the reach run, the bundle and the release are this entry's
(below).

### Human-reviewed (T184)
An entry is HUMAN-REVIEWED when its mark in force is `verified` — every check (T R P A) answered y —
or, a zone entry, when every rule's is. `GET /api/entries/{id}` and each queue row carry
`reviewed: {by: [reviewer, …], on: <latest date>, holds}` (null when never reviewed): `holds` is false
once the entry's fingerprint moved (text, rules or extent — the mark's typed stale reasons, T97), when
it is back in the queue for a re-check (`verify=recheck`). The screen shows "Human-reviewed by X on
date", and "Was human-reviewed … changed since: re-check it".

### One vintage on screen (T133)
`GET /api/entries/{id}` carries `vintage: {state: current | "awaiting build", reasons, splits,
splits_checked: anchors | ids, commands}` (`pipeline.regs.review.vintage.entry_state`): `current`
only when the reach run's `report.json` `entry_digests[id]`, the bundle's `meta` `entry_digests[id]`
and the entry's digest now (`io.entry_digest`) agree, the newest release's manifest `bundle` states
that bundle's `entries_digest` + `reach_digest`, the reach run and the bundle were made on the
served atlas, and every split the entry reads is in that atlas as authored (`split_anchors.json`;
an atlas built before that record checks split ids only — `splits_checked: ids`). No bundle is
awaiting build. Queue rows carry `vintage` and `vintage_reasons`; `/api/marks/splits` carries each
split's `vintage`. Re-read from disk by the files' stamps on every request: a rebuild run in a
terminal clears it with no restart. `CURATION_REACH_DIR` / `CURATION_RELEASE_ROOT` point the reach
run and the release root at copies (tests).

### GET /api/build
→ `{built, entries: [{entry_id, region, name, reasons, splits}], splits: [{split_id, reasons}],
splits_checked, commands}` — the pending-build panel: everything awaiting its build and what the
human runs (`scripts/heavy.sh build 30 -- python -m pipeline build --no-tiles` for an entry fix;
the atlas build, binding diff and promote first when a split or the atlas is why). Never runs it.

### GET /api/marks?target=&scope=
→ the marks in force (the last per target), each with `status` (`verified | wrong | unsure |
deferred | stale`) and `reasons` (`book text changed | rule changed | binding moved | answer changed
| awaiting build`) against the target NOW. `GET /api/entries/{id}` carries `marks` (its entry and
rule marks, the same shape).

### GET /api/marks/splits
→ `{split_id: {status, verdict, reasons, vintage}}` for every split in splits.json — the OLD split
marks' status (history since T184; most read `unmarked`). A split whose old mark went stale is one
still to verify (`tiers.splits_to_verify`, the `split_to_verify` pre-check).

### The review screen (T134)
- `GET /api/entries/{id}/review?date=` → `{entry_id, name (the RAW name — the heading),
  display_name, kind, region, tier, risks, evidence, coverage, vintage, answer, date, rules,
  prechecks, parse_findings, scope, run}`. T135: `prechecks` `[{code, severity: A | B,
  rule_id, what}]` (`precheck.prechecks`, the gate included; an ambiguous cut's candidates and
  pick are its `ambiguous_cut` / `cut_picked` finding — T189 removed the second copy,
  `ambiguous_cuts`); T189: `scope` `{entry: {items, where, tributaries: {yes, stated}, carve_outs,
  name_except, rules_walking}, rules: {rule_id: {where, book_says, items, splits, in_part, side,
  unresolved, tributaries: {yes, only, why}, carve_outs, exceptions}}}` — each extent in plain
  words (`pipeline.regs.review.scope_words`, the bundle's place-namer); `parse_findings` the second-pass agent's lines (`io.read_review_findings`; none exist yet); `run`
  `{run: digest | null, why}`.
  `evidence`: `{kind: row | passage | none | missing, printed, pdf_page, boxes, found, total, why,
  crop_url, page_url, row_image}` — a water row's box from `row_boxes.json`
  (`CURATION_ROW_BOXES`; `missing` with the command when there is none), a zone passage found by
  text on its printed page (`found` / `total` words). `coverage`: `{segments: [{text, kind: quoted |
  unquoted | gap}], unquoted_chars, missing: [record ids whose quote is not in the row]}`.
  `answer` / each zone rule's `answer`: `{url, item, part, sections, via}` — the app's start link
  (`CURATION_APP_URL`, default `http://127.0.0.1:39218/`), or `{url: null, why}` while awaiting
  build. Each zone rule carries `impact` ("binds N sections in region R", the bundle's count).
- `GET /api/entries/{id}/evidence/crop.png` — the row (water column above regulation column) or
  passage at 2.6 px/pt; `…/evidence/page.png` — the printed page with it marked. `404` naming the
  command when the sidecar is missing.
- `GET /api/review/progress` → `{tiers: [{tier, total, done, stale, todo}], overall, facts}` (done =
  a mark in force: verified, wrong, unsure, deferred; `facts` = `{run, why}`, the reach run the
  risk factors were read from). Entries only: the tiers are P1 Z P2 P3 P4 sweep.
- `GET /api/entries/{id}/splits` (T184) → EVERY SPLIT THE ENTRY'S RULES NAME (its scope and
  licensing records too), as a FeatureCollection of Points: `properties` `{split_id, label (the
  built label, else splits.json's, else the boundary's), water (its splits.json waterbody; a lake
  boundary, the item it is on), name ("WATER: label"), rules: [rule ids naming it], parts: ["entry
  scope" | licensing ids], to_verify (an open auto-proposal or a "Verify:" note), grade, item_id}`,
  plus `unplaced: [{…the same, why}]` for an id the served atlas has no point for — listed, never
  dropped. `404` for an unknown entry. The map draws and labels each (the proxy check of a split).

## What an angler is told (the live bundle)

Read from `data/generated/bundle/bundle.sqlite` — the LAST BUNDLE BUILD, not the entry file.

### GET /api/entries/{entry_id}/bundle
→ `{vintage, awaiting, bundle: {built, version, …}, in_bundle, bundle_matches, mismatches: {rule_id:
[what differs]}, unchecked: [rule_id], rules: [{rule_id, label_bundle, label_now, n_sections,
n_sections_now, unresolved}], licensing: [{kind, id, label, unresolved, n_sections, n_waters, waters:
[{name, n}]}]}`. `bundle_matches` (`vintage.bundle_matches`, T133) compares each rule's label AND its
section count — the bundle's (`fingerprint.rule_section_counts`) beside the reach builder's on the
served atlas now (`n_sections_now`; `None` → `unchecked` for a rule one entry alone cannot count).
Labels alone called Tacheeda / Twin / Main Lake a match while the bundle bound one lake of two. For
an entry AWAITING BUILD only `{vintage, awaiting: true, bundle_matches: false}`: the bundle's copy is
another vintage's.

### GET /api/entries/{entry_id}/answer/waters?q=&limit=60
→ `[{item_id, name, n_sections, pieces: [{sid, n_sections, rules: [{rule_id, via}]}]}]` — the waters
the entry reaches in the bundle (`"(unnamed streams)"` for sections no named item holds), each split
into pieces that answer to one rule set; `sid` is a representative bundle section (a handle within
one bundle vintage — never stored). `409 {vintage}` while the entry awaits its build (T133).

### GET /api/answer?sid=&date=YYYY-MM-DD&fish=
→ `{sid, date, fish, waters, tidal, rules: [{entry_id, rule_id, entry_name, state, type, label,
verbatim, via, undrawn_part, standing}], licensing: [{kind, entry_id, id, via, label}], bundle}` —
`pipeline.deliver.bundle.read.effective_rules`, unchanged. `fish` is one leaf code; a group → 422.

## The book

`source_pages` are PRINTED page numbers. `GET /api/synopsis/pages` → `{printed: pdf_page}` read off
the repo copy's footers (`pipeline.common.synopsis_pages.printed_to_pdf` — the one map, also
shipped as the UI export's `guide.pdf_pages`); `GET /api/synopsis.pdf` serves that copy (link `#page=<pdf_page>`);
`GET /api/synopsis/page/{printed}.png` renders the whole page.

---

## Map

### GET /api/items/{item_id}/geojson
Stream sections + curated split points (+ a point inside each lake boundary) in EPSG:4326, to overlay the web-mercator PMTiles basemap.
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

### GET /api/entries/{entry_id}/rules/{rule_id}/resolved?limit=6000
**What is IN this rule, and what is OUT** — from `pipeline.atlas.reach.build.build_reach`, the same call the
artifact build makes, so what a curator confirms here is what ships.

Exists because the map cannot answer this from the item layer. It draws the entry's own item geometry
and highlights sections inside it, which works only while a rule stays inside its own water. Two
common shapes do not:

| shape | example | bound | in the entry's own geometry |
|---|---|---|---|
| `within(area)` | Pitt River within Garibaldi Park | 466 | **18** |
| tributaries | "between A and B, including tributaries" | 34 | 15 |

So 448 sections of the Garibaldi rule — 96% of it — had no geometry loaded and could not appear on
the map at all.

```jsonc
{
  "outcome": "bound", "within_area": true, "wants_tributaries": true,
  "n_direct": 18,     // the extents alone, before expansion
  "n_added": 448,     // what the builder ADDED: the area's other waters, or the tributary walk
  "n_total": 466,     // IN — the exact set the build ships
  "n_offitem": 448,   // bound sections the item layer never had; they exist only in this payload
  "n_excluded": 0,    // OUT — removed by EXCEPT carve-outs
  "carve_outs": [ { "extent": {...}, "resolved": true, "n_sections": 12, "above": 197 } ],
  "sections": ["..."], "truncated": false, "limit": 6000,
  "geojson": { "features": [ /* kind: "reach_extra" (IN) | "reach_excluded" (OUT) */ ] }
}
```

The map draws `reach_extra` green and `reach_excluded` red, so an EXCEPT is visible **as an
exclusion** rather than inferable from a smaller total. `above` is everything upstream of the water a
carve-out names, which an EXCEPT always takes with it — note the block walk will not cross a
`continuation` edge out of the excepted stretch, which is what makes a partial carve-out mean what it
says.

`sections` is deliberately NOT trimmed to one level of tributaries. Showing less than ships is the
failure the `build_reach` docstring records, and it costs nothing to avoid — the builder has already
walked it.

**Button-driven.** The resolve is cheap (median 0.04 s); the geometry is not — the largest rule is
~3,200 sections, ~4 s and ~12 MB. `truncated` is reported, never hidden.

### GET /api/row-image/{filename}
The source synopsis row-crop PNG (`data/generated/regs/extraction/row_images/`). Filename must match
`[A-Za-z0-9_]+\.png`.

### GET /basemap/bc.pmtiles
Serves `data/bc.pmtiles`, honouring HTTP Range requests (the pmtiles protocol needs byte ranges).

---

### UI must-haves
- The book first (the row / passage, legible), then the checklist (T R P A) with the verdict bar.
- Every rule's generated label and quote, WHERE it binds (its splits by name), the builder's answer.
- **Every split the rules name, labelled on the map** (T184) — the proxy check of a split.
- The item's **bindable boundaries** (curated splits flagged via `boundaries[].curated`), spanning
  every item a combined entry covers; **unused curated splits** (`unused_curated_splits[]`).
- **Show reach** per rule, with the straddling / ambiguous-cut warnings surfaced.
- **Related entries** — jump to the other synopsis rows over the same water.
- **Human-reviewed** on the entry and in the queue (T184); the queue in the review order.
