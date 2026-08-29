# Curation Review — parsed-entry review & confirm tool

A small, **local, one-off** app for a human to review the LLM-parsed regulation entries, see all the
context (original reg text, parsed rules, split bindings, and the stream on a map), and **confirm or
correct** each one. Confirming sets `locked: true` on the entry so a future re-parse never overwrites
hand-checked work.

> Status: **built and in use.** This doc is both the spec and the reference.

## Run it

```bash
bash curation-review/run.sh          # backend :8787 + frontend :5173, Ctrl-C stops both
```

Then open **http://localhost:5173**. Local only — no LLM calls, no credits.

The build it serves is `config.yaml` → `output.review_build` (currently `output/v2/full`);
`reuse.py` and the in-app rebuild button both read `project_config.review_build_dir`, so
repointing the app at another build is a config edit, not a code change.

⚠️ **The in-app rebuild button overwrites the build it is serving.** It deletes and rewrites
`graph.gpkg`, so the map is broken for the several minutes that takes. To verify a build
before adopting it, build to a staging directory and swap — see `pipeline/docs/NEXT.md` §0.

---

## Why

The parser (`pipeline/parsing/`) turns each synopsis row into an `Entry` with `rules`, each bound to a
reach by `extents` (op + curated split ids). The parse is validated structurally, but a rule can be
**confident-but-wrong** (bound to the wrong reach, wrong species, missed a restriction). A human needs
to eyeball each entry against the source text — and, where the parser flagged uncertainty
(`needs_review`, `unresolved_locators`, `registry_status: no_registry`), make the call the model
couldn't. This tool is that review surface.

## What the curator does per entry

1. Read the **original reg text** (`regs_verbatim`) next to the **parsed rules**.
2. Check each rule's **binding**: `op` + which **splits** (boundaries) it selected, species, dates,
   tributary scope.
3. See the **stream on a map** with its split points, to sanity-check the reach.
4. Then one of:
   - **Confirm** → set `locked: true` **and stamp `reviewed_by` + `reviewed_at`** (frozen; re-parse
     won't touch it).
   - **Edit** → fix an `extent`/`matched`/`sections_override`/species, re-validate, then lock.
   - **Attach registry** (for `no_registry` entries) → search + pick a registry item, bind extents,
     confirm — the no-registry rows are triaged **here**, not in a separate pass.
   - **Flag** → leave a note in `audit_log` and move on (still unlocked).

> **Entry-model change needed:** add `reviewed_by: str` and `reviewed_at: str` (ISO) to
> `pipeline/parsing/entry_models.py::Entry` (default empty). Confirm sets `locked=true` + both stamps;
> ingest already preserves `locked` entries, so the stamps ride along untouched.

---

## Data it reads / writes

| Source | Path | Role |
|---|---|---|
| Entries (read **and write**) | `pipeline/parsing/entries/region-*.json` | the review surface; the app edits `locked`, extents, `matched`, `audit_log` |
| Registry (read) | `output/v2/full/registry.json` | item identity, **`boundaries`** (the bindable splits), `variants`, `mus`, `section_ids` |
| Split detail (read) | `output/v2/full/splits.resolved.json` | `{split_id, blk, route_measure, label, anchor_type}` — split metadata |
| Geometry (read) | `output/v2/full/graph.gpkg` | layers: `streams`, `split_points`, `lakes`, `areas` — served as GeoJSON for the map |
| Batch parse artifacts (read, optional) | `output/parse/{batches,responses,reviews}` | show the reviewer's findings + the raw parse for provenance |

**Write model:** edits go back into `region-*.json` through the **same validators the pipeline uses**
(`pipeline.parsing.entry_models.Entry` + `validate_entry_splits`) so the app can never write an invalid
entry. Writes are atomic (temp file + rename). **Git is the backstop, committed manually** by the
curator before/after a session — the app does not auto-commit.

---

## Screens

```
┌───────────────────────────────────────────────────────────────────────────┐
│  [region ▾] [status ▾: needs_review · no_registry · unlocked · flagged ]     │  filter bar
├──────────────┬────────────────────────────────────────────────────────────┤
│ ENTRY LIST   │  ENTRY DETAIL                                                │
│ (queue)      │  ┌─ Identity: name · region · MUs · matched item ─────────┐ │
│              │  │  registry_status badge (matched / NO REGISTRY)          │ │
│ ▸ Chemainus  │  ├─ Original regs (regs_verbatim)  │  Parsed rules ────────┤ │
│   needs_rev  │  │  "No fishing between Copper      │  r1 closure           │ │
│ ▸ Atnarko ✓  │  │   Canyon Falls and the signs…"   │   between [falls,signs]│ │
│ ▸ Frog  ⚠NR  │  │                                  │   species: all        │ │
│   …          │  │                                  │   ⚠ needs_review: …   │ │
│              │  ├─ Splits menu (this item's bindable boundaries) ─────────┤ │
│              │  │  falls · signs_100m · bannon_confluence  (pick to bind)  │ │
│              │  ├─ MAP: stream + split points, selected split highlighted ─┤ │
│              │  └─ [Confirm & lock]  [Save edit]  [Flag]  [Skip →]  ───────┘ │
└──────────────┴────────────────────────────────────────────────────────────┘
```

- **Queue (left):** entries for the chosen region, sorted so the ones needing attention float up —
  `no_registry` → rule `needs_review`/`unresolved_locators` → reviewer high/med findings → unlocked →
  locked (done). Each row shows a status badge and lock state.
- **Detail (right):** original vs parsed side-by-side; per-rule binding shown as human text
  ("upstream of Hunlen Falls") **and** the raw `op`+split ids; the item's full split menu; the map; the
  action bar.

## Map (from `graph.gpkg`)

For the selected entry's `matched` item, the backend queries `graph.gpkg` by the item's `blk`/`wsc`
(lazy, `where=` filtered — no 2 GB pickle load) and returns GeoJSON:

- **`streams`** — the reach line(s) for the waterbody.
- **`split_points`** — the cut points, labelled; the split(s) a rule references are highlighted, with an
  up/down/both indicator for the `op` (upstream_of / downstream_of / between).
- **`lakes`/`areas`** — polygon context when relevant.

Renderer: **MapLibre GL** (open source, no API key, CSP-friendly).

**Per-section colouring is the goal** (highlight exactly which sections each rule covers, so a wrong
reach is obvious). That requires resolving `op + splits + tributary flag → section ids` — i.e. the
**resolver**, which is not built yet (`pipeline/docs/SESSION-HANDOFF.md §3`). Two ways to get there:

- **(preferred)** build the real resolver in `pipeline/` (it's needed for the product anyway) and have
  this tool call it — one implementation, reused; **or**
- ship a **light resolver inside the tool** first (section bounds are already in the graph: split →
  bounding sections, then upstream/downstream/between by route measure) to unblock colouring, and fold
  it into the real resolver later.

Either way, colouring is in-scope for this tool (see phases). A splits-only map is the fallback if the
resolver slips.

---

## Tech stack (proposed — keep it small)

- **Backend:** Python **FastAPI**, run locally (`uvicorn`). It imports the repo's own modules
  (`pipeline.parsing.entry_models`, `pipeline.parsing.validate`, `pipeline.registry`) so load / edit /
  validate / save all reuse pipeline code — no logic is re-implemented. Serves the entries API + the
  per-item GeoJSON.
- **Frontend:** a single lightweight page — plain HTML/JS or a minimal Vite setup — with MapLibre GL for
  the map. No heavy framework; this is an internal tool for one user.
- **Why not reuse `webapp/`?** That's the public product (tiles, R2, deploy). This is a throwaway
  internal editor that needs local file **write** access and pipeline validation — cleaner as its own
  tiny app.
- **Alternative considered:** Streamlit (fastest to stand up, weaker map/side-by-side layout). FastAPI +
  a static page is chosen for the two-pane + map UX and direct reuse of the models.

### Proposed API (FastAPI)

| Method · path | Does |
|---|---|
| `GET /api/regions` | list regions + counts by status |
| `GET /api/entries?region=&status=` | queue: entries with status/lock flags |
| `GET /api/entries/{entry_id}` | full entry + its matched item's boundaries/variants + reviewer findings |
| `GET /api/items/{item_id}/geojson` | stream + split_points GeoJSON for the map |
| `GET /api/items/search?q=` | registry search (for attaching an item to a `no_registry` entry) |
| `PUT /api/entries/{entry_id}` | validate (Entry model + split ids) then write back to `region-*.json` |
| `POST /api/entries/{entry_id}/lock` | set `locked: true` (confirm) |

---

## Build phases

- **MVP (text + splits + confirm):** queue, side-by-side reg vs parsed, per-rule binding shown in
  human + raw form, the split menu, **Confirm** (lock + `reviewed_by`/`reviewed_at`) and **edit-extent**
  with server-side validation, atomic write-back, plus the **registry-attach flow** so `no_registry`
  entries are triaged here. No map yet. This alone makes review fast.
- **Phase 2 (map + colouring):** `graph.gpkg` GeoJSON endpoint + MapLibre panel; **per-section
  colouring** via the resolver (real resolver preferred; light in-tool resolver as fallback), split
  markers + op direction.
- **Phase 3 (nice-to-have):** show reviewer findings inline; bulk-confirm a filtered set; keyboard-driven
  review flow.

## Explicitly out of scope

- Not the public webapp; not deployed; local only.
- Does **not** run the parser — it reviews the parser's output. (It *may* call the resolver for map
  colouring; see Map.)
- No auth / multi-user (single curator, localhost).
- No new geometry computation — reads what the build already produced.
- No auto-commit — the curator commits git manually.

## Decisions (resolved)

1. **Confirm** sets `locked: true` **and** stamps `reviewed_by` + `reviewed_at` (new Entry fields).
2. **Git** is committed **manually** — no auto-commit.
3. **Per-section colouring is wanted** — Phase 2 uses the resolver (build it, or a light in-tool version
   first); splits-only is only the fallback.
4. **`no_registry` entries are handled in this tool** (attach a registry item + verify the parse), not a
   separate pass.

## Open questions

- Resolver: build the real one in `pipeline/` now (needed for the product anyway) and reuse it here, or
  ship the light in-tool resolver first to unblock colouring? (affects Phase 2 sequencing)
