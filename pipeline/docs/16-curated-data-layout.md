# 16 — One home for curated data, and paths from config

> ## ⚠️ SUPERSEDED AND IMPLEMENTED — 2026-09-03
>
> The move happened, and it went further than this document proposed. Curated data lives under
> **`data/curated/`**, not `pipeline/curated/`, beside `data/source/` and `data/generated/` — so
> the three kinds of data are visible in one listing and `pipeline/` is purely code. It is also
> grouped by DOMAIN (`waters/`, `regulations/`, `gauges/`) rather than flat.
>
> This document is kept for its reasoning and its reference counts, both of which held up. What
> it did not have is the third category: **promoted** data, machine-produced and human-approved,
> which looks generated and is not. See `data/curated/README.md` and AGENTS.md rules 35-40.
>
> Original status line follows.

**Status: PLAN ONLY. Not implemented.** Written 2026-09-02 while another agent held `build.py`,
`graph/`, and the hydro/gauge work; the move touches those files, so it waits.

> **Executing this?** The line-by-line surface — every real path resolution, the greps that
> only *look* like work, and the order — is in
> [`HANDOFF-curated-layout.md`](HANDOFF-curated-layout.md). It corrects two counts below.

## The problem

Curated, hand-authored data — the stuff a human decided, that no rebuild can regenerate — is
scattered across `pipeline/` with no marker distinguishing it from code:

```
data/curated/waters/splits.json                404K   curated cut-points
data/curated/waters/name_variants.json         964K   curated names (3,676 entries)
data/curated/waters/added_lakes.geojson        4.0K   curated non-FWA lake polygons
data/curated/waters/added_streams.json   1.4M   frozen added-streams dataset
data/curated/waters/areas.json                 8.0K   curated area definitions
data/curated/gauges/matches.json           720K   gauge -> stream matches
data/curated/waters/ungazetted.json            4.0K   NO CODE REFERENCES IT — see "loose ends"
data/curated/regulations/overrides.json    181K   curated name -> item bindings
pipeline/regs/parsing/entries/           8 files provincial synopsis entries
pipeline/regs/dfo_salmon/entries/        9 files DFO salmon entries
```

Two costs, both already paid once:

* **Paths are hardcoded, in five different shapes.** For `splits.json` alone: 7× the literal
  `"data/curated/waters/splits.json"`, 3× `<something> / "splits.json"`, 2× `parents[1] / "splits.json"`, 1×
  `parents[2] / ...`, 1× `Path("data/curated/waters/splits.json")`, and exactly 1 that goes through config.
  `config.yaml` already carries the scar from the same class of bug, on `review_build`: *"These were
  two independent hard-codings of `output/v2/full` in reuse.py and rebuild.py: the app could rebuild
  one directory and read another, and neither would say so."*
* **Curated data is indistinguishable from generated data.** Nothing in the tree says
  `splits.json` is irreplaceable while `output/v2/full/` is 27 minutes of CPU. A `--splits` flag
  with no default silently dropped all 376 curated cuts from three full builds because nothing made
  the omission loud. That is fixed, but the shape that allowed it is structural.

## Proposed layout

```
pipeline/curated/                  # everything a human authored; nothing here is regenerable
    splits.json
    name_variants.json
    overrides.json                 # moves from pipeline/regs/matching/
    areas.json
    added_lakes.geojson
    added_streams.build.json
    gauge_match.json
    entries/
        synopsis/                  # from pipeline/regs/parsing/entries/    (region-1..8.json)
        dfo_salmon/                # from pipeline/regs/dfo_salmon/entries/ (region-1..8,5a,5b.json)
```

`entries/` is nested rather than split into two top-level dirs because the two sets are the same
KIND of thing — per-region curated regulation records — and the review app already serves both.
They stay separate *files* with separate models (`Entry`/`Rule` vs `EntryFile`/`Location`/`Binding`);
only the location is shared. Their one real coupling — DFO importing the provincial `Extent` model
rather than re-declaring it — is a code dependency and is unaffected.

## Config

`project_config.py` already has the machinery: a singleton over `config.yaml`, `get_path(*keys)`,
and ~15 typed properties. Add a `curated:` tree and properties beside the existing ones.

```yaml
# config.yaml
curated:
  # Hand-authored data. NOTHING here is regenerable: a rebuild reads it, never writes it.
  # Losing a file here loses curation that cost human time; losing anything under output/ costs CPU.
  base: "pipeline/curated"
  splits: "pipeline/curated/splits.json"
  name_variants: "pipeline/curated/name_variants.json"
  overrides: "pipeline/curated/overrides.json"
  areas: "pipeline/curated/areas.json"
  added_lakes: "pipeline/curated/added_lakes.geojson"
  added_streams: "pipeline/curated/added_streams.build.json"
  gauge_match: "pipeline/curated/gauge_match.json"
  entries:
    synopsis: "pipeline/curated/entries/synopsis"
    dfo_salmon: "pipeline/curated/entries/dfo_salmon"
```

```python
# project_config.py — one property per file, mirroring builds_dir / fwa_data_gpkg
@property
def splits_path(self) -> Path: ...
@property
def name_variants_path(self) -> Path: ...
@property
def overrides_path(self) -> Path: ...
@property
def synopsis_entries_dir(self) -> Path: ...
@property
def dfo_entries_dir(self) -> Path: ...
```

**Every default becomes a config read.** A CLI `--splits` stays, as an override for a one-off, but
its default is `get_config().splits_path` — never `None`, never a literal.

## What it touches

Counted by `grep -rl` over `*.py|*.ts|*.tsx|*.sh|*.yaml|*.md`, excluding `.venv`, `node_modules`,
`__pycache__`:

| file | refs | where |
|---|---|---|
| `splits.json` | 32 | docs 16 · hack 8 · frontend 7 · dfo_salmon 6 · tests 5 · splits 3 · backend 3 · build/tools/reach/parsing/hydro 1 each |
| `name_variants.json` | 13 | |
| `areas.json` | 6 | |
| `gauge_match.json` | 5 | **owned by the other agent's hydro work — coordinate** |
| `added_streams.build.json` | 4 | |
| `added_lakes.geojson` | 3 | |
| `ungazetted.json` | 0 | see loose ends |
| `pipeline/regs/parsing/entries/` | 8+ | incl. `curation-review/backend/reuse.py` |
| `pipeline/regs/dfo_salmon/entries/` | 8+ | incl. `curation-review/backend/reuse.py` |

Roughly **60 code references** plus ~20 doc mentions. The docs matter: `AGENTS.md` rule 2 names
`pipeline/regs/parsing/entries/*.json` explicitly, and several design docs cite paths in prose.

Areas needing care, in order of risk:

1. **`curation-review/backend/reuse.py`** — reads BOTH entry dirs and `splits.json`, and is the one
   place the app and the builder are required to agree (AGENTS #16: *"3,038 of 3,038 rules
   identical"*). Any divergence here is invisible until a curator hits it.
2. **`curation-review/frontend`** — 7 refs, but they are UI strings and comments, not filesystem
   paths (`SplitEditor.tsx`, `api.ts`, `types.ts`). The frontend reaches the file through the
   backend API. Update the prose; no behaviour change.
3. **`pipeline/hack/`** — 8 refs across one-off scripts. Lowest risk, easy to miss.
4. **`pipeline/atlas/build.py`** — `_DEFAULT_SPLITS`, `_ADDED_LAKES_GEOJSON`, `_ADDED_STREAMS_JSON`, the
   `--name-variants` default. Shared with the other agent right now.
5. **Tests** — `test_splits.py`, `test_entryfiles_valid.py`, `test_dfo_salmon.py`,
   `test_added_lakes.py` all resolve paths via `parents[N]`, which is exactly the brittle shape
   being removed.

## Migration order

1. Add the `curated:` tree and the properties. **Point them at the CURRENT locations.** Nothing
   moves; nothing breaks.
2. Convert references to the config accessors, one area at a time, running the suite between each.
   This is the bulk of the work and it is all reversible.
3. Verify **zero** literal paths remain: `grep -rn '"pipeline/[a-z_]*\.json"'` returns nothing
   outside `project_config.py` and docs.
4. `git mv` the files. Flip the config values. One commit, one revert if wrong.
5. Update `AGENTS.md`, `pipeline/README.md`, and the design docs that name paths in prose.
6. Add a guard test: every path in `curated:` exists, and no module resolves a curated file by
   `parents[N]`.

Step 4 is last on purpose. Doing it first would break ~60 references at once and leave the tree
un-runnable while they were fixed one by one.

## Risks

* **A missed reference fails at RUNTIME, not import.** `Path("data/curated/waters/splits.json")` that no longer
  exists gives an empty load, not a crash — the `--splits` incident again, where a build with zero
  curated cuts looked healthy. Mitigation: step 3's grep must return empty before step 4, and
  `load_split_defs` should raise on a missing file rather than return `[]`.
* **The other agent holds `build.py`, `graph/`, and hydro.** `gauge_match.json` is theirs and moving
  it mid-flight would conflict. Do this when their task is done.
* **`git mv` of `entries/` breaks in-flight curation.** Anyone with local edits to an entry file gets
  a conflict. Do it on a clean tree.
* **Build reproducibility spans the move.** A build made before it reads the old paths; parity
  between a pre-move and post-move build should be IDENTICAL — that is the acceptance test.

## Acceptance

* `pipeline/tests/` green.
* `build_parity <pre-move build> <post-move build>` reports **IDENTICAL** — same items, sections and
  boundaries. If the move changed what the build reads, this catches it.
* `grep -rn '"pipeline/[a-z_]*\.json"' --include=*.py` empty outside `project_config.py`.
* The review app loads an entry from each set and resolves a reach.

## Loose ends found while surveying

* **`data/curated/waters/ungazetted.json` has zero code references.** The only "ungazetted" mentions are a
  matcher docstring (`archive/` is old reference code and out of scope). Either it is dead and
  should be deleted, or
  something stopped reading it and that is a bug. Decide before the move rather than relocating a
  file nobody loads.
* **`data/curated/regulations/overrides.json` is curated but lives with matcher code**, unlike every other
  curated file. The move fixes that, but it is the file with the most concurrent edits (DFO
  curation) — sequence it carefully.
