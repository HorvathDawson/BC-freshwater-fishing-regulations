# stream_sections/oneoff — one-off / bootstrap & curation tooling

Run-once and hand-curation tooling, kept for reproducibility but NOT part of the build.
**Run everything from the repo root** with `.venv/bin/python -m stream_sections.oneoff.<script>`.

## Bootstrap / reports
| Script | What it does | Output |
|--------|--------------|--------|
| `name_variants_compile.py` | Bootstrap the unified name-variations file (feature_display_names + overrides + anglerinfo). | `stream_sections/name_variants.json` |
| `complex_regs_report.py` | Scan overrides + parsed synopsis for section-language / tributary / multi-rule complexity. | `output/v2/complex_regulations.md` |
| `bridge_structural.py` | Structural pass on `bridge_road_km` locators: split `from A to B` into `-a`/`-b` (per-endpoint kind), reclass, note shared anchors. `apply` mutates. | writes `14-locators-to-curate.json` |
| `osm_bridges.py` | Candidate coords for man-made crossings (highway/road/rail/**power line**/**dam**) via OSM Overpass ∩ MU-clipped FWA river. `report`→`output/osm_candidates.md`; `apply`→`[osm-candidate]` notes. FWA is used for the river; OSM ONLY for features not in the gpkg (see `../docs/14`). | `output/osm_candidates.{md,json}` |

---

## Curation tooling — `14-locators-to-curate.json`
Point/boundary curation for fishing-reg locators. Each row has `anchor_kind` (split type: `point |
confluence | lake | line | area_boundary | lake_io | buffer | not_a_split | unclassified`), a
`resolver_hint` (old bucket: `tributary | falls_obstacle | dam_weir_fence | bridge_road_km | …`),
a `status` (`todo | curated | manual | not_applicable | deferred | auto`), and a free-text `notes`
that carries research + `[auto-proposal H|M|L] … Candidate coord [lon,lat]` lines.
Full method & gotchas: **`../docs/17-manual-review-runbook.md`**.

### `waterbody_splits.py` — group splits by reg entry + completeness check
Pivots the locator rows by their source reg entry (waterbody+MU+reg), links each reg-text boundary
(`synopsis_parsed.json` rules) to the curated row(s) that resolve it, and flags any boundary with no
row (`MISSING`). Regenerable view over `14-locators-to-curate.json` (source of truth untouched).
Full model & workflow: **`../docs/14-waterbody-split-curation.md`**.
```bash
.venv/bin/python -m stream_sections.oneoff.waterbody_splits            # write cards + summary
.venv/bin/python -m stream_sections.oneoff.waterbody_splits incomplete # entries not fully resolved
.venv/bin/python -m stream_sections.oneoff.waterbody_splits show "DEAN RIVER"
.venv/bin/python -m stream_sections.oneoff.waterbody_splits regs-md    # synopsis table w/ live locators bolded inline
```

### `curation_status.py` — progress + work queue + review
```bash
# progress table (per anchor_kind) + the next 10 open items
.venv/bin/python -m stream_sections.oneoff.curation_status
.venv/bin/python -m stream_sections.oneoff.curation_status next 15 --hint bridge_road_km
.venv/bin/python -m stream_sections.oneoff.curation_status show  <id>          # one row as JSON
.venv/bin/python -m stream_sections.oneoff.curation_status review not_applicable --kind confluence   # audit a bucket
.venv/bin/python -m stream_sections.oneoff.curation_status defer <id> --reason "…"   # park to back of queue
.venv/bin/python -m stream_sections.oneoff.curation_status undefer <id>
```
`--kind` filters by split type, `--hint` by the granular bucket. Deferred rows sort last.

**Write one row (online, direct-to-doc):**
```bash
.venv/bin/python -m stream_sections.oneoff.curation_status annotate <id> \
    --status curated --coord=-118.634,49.148 --blk 356526465 --label "…" --note "…"
# NOTE: use --coord=-LON,LAT (equals form) so argparse doesn't read the leading '-' as a flag.
```

### Offline labelling (no service / no API) — CLI label loop
Produces a **decisions file** the doc consumes later; needs no network to label.

**Review order — assume nothing.** Only `curated` and `manual` are trusted. Every other
auto-decided row (was `not_applicable` / `deferred` / `auto`, plus the `not_a_split` rows) sits in
status **`likely_na`** and is surfaced **first** (⚑ banner, prior status kept in a `[review · was:X]`
note) so you re-decide each before touching real curation:
`x`=not-a-split(→n/a) · `d`=defer · `o`=auto(lake edge) · `m`=manual · `y`/`c`=curate ·
`k <anchor_kind>`=reclassify as a real split. After the ⚑ pile come the easiest real rows
(a candidate coord), then the rest.

**A. Interactive CLI (best beside QGIS).** Walks the queue in that order, shows *what to find* +
OSM/Google/Satellite links + `target` (blk/wsc/wbk to locate in QGIS/FWA). Decisions go to a
**separate file** — the live doc is untouched.
```bash
.venv/bin/python -m stream_sections.oneoff.curation_status label --hint dam_weir_fence --easy
#   per item:  y = accept candidate         c -125.1,50.2 = set coord (paste from QGIS)
#              d[ reason] = defer            x[ reason] = not a split      m[ reason] = manual
#              t wbk=..|blk=..|wsc=.. = set target      n <note>      a <text> = flag for an agent
#              u = undo this row             enter/s = skip            q = quit
#   --easy hides rows without a candidate; --out <path> overrides output/review_decisions.json
```

**Back in service — apply the decisions into the doc:**
```bash
.venv/bin/python -m stream_sections.oneoff.curation_status apply                       # output/review_decisions.json
.venv/bin/python -m stream_sections.oneoff.curation_status apply path/to/decisions.json   # e.g. the HTML export
#   correct->curated(+coord) · wrong+coord->curated · not_a_split->not_applicable
#   defer->deferred · manual->manual · for_agent-> left todo & reported as NEEDS AGENT
```
Rows you can't finish solo (two-boundary "A→B" splits, confluence `wsc`, offsets) — mark them `a`
(for-agent) in the CLI or leave a `[for-agent]` note; an agent finishes them (grep notes for `[for-agent]`).

### `resolve_lake_offsets.py` — auto-resolve "N km below <Lake>" boundary-sign points
Finds each lake's outlet on the target stream (nearest `(blk, poly)` pair to beat name collisions;
clean boundary-crossing else nearest-point) and walks the reg's downstream offset. Prints proposals;
write them with `annotate`.
```bash
.venv/bin/python -m stream_sections.oneoff.resolve_lake_offsets
```

### Review comments in the docs (offline reading → reviewable discussion)
One universal convention: an **HTML comment** — invisible in rendered markdown, present in the
source, greppable in any file. Drop them anywhere as you read:
```
<!-- @REVIEW: this confluence looks like the north fork, double-check -->
<!-- @Q: why is Dean's canyon 3–5 km from the mouth, not the upper canyon? -->
<!-- @BLOCKER: don't author the Skeena splits until I confirm the reach labels -->
```
An agent answers inline right below, so it reads as a thread: `<!-- @REPLY: … fixed in <commit> -->`.
Collect them back in service (this makes the discussion reviewable):
```bash
.venv/bin/python -m stream_sections.oneoff.review_comments          # all @REVIEW/@Q/@BLOCKER/@REPLY
.venv/bin/python -m stream_sections.oneoff.review_comments --open   # only unanswered
```
Tags: `@REVIEW` (note), `@Q` (question), `@BLOCKER` (must-fix-first), `@TODO`, `@REPLY` (answer).
For the locators JSON specifically, prefer the `label` loop's `n <note>` / `a <for-agent>` instead.

## Typical away-from-service loop
1. `curation_status label --hint <bucket> --easy` → label offline.
2. Decisions accumulate in `output/review_decisions.json` (safe to copy around).
3. On return: `curation_status apply` → then hand-finish any `NEEDS AGENT` rows → commit.
