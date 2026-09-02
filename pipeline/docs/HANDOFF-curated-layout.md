# Handoff — implementing the curated-data restructure

**For the agent who executes `16-curated-data-layout.md`.** That document is the plan and the
rationale; read it first. This one is the execution surface: the exact lines to change, the ones
that only *look* like they need changing, and the order that keeps the tree runnable throughout.

Written 2026-09-02 by the agent that surveyed it. Nothing has been moved.

---

## 0. Before you start

* **Clean tree, and confirm nobody is mid-curation.** `git mv` of `entries/` conflicts with any
  local edit to an entry file.
* **The other agent's task must be done.** They hold `build.py`, `graph/`, and the hydro/gauge work;
  `gauge_match.json` is theirs. They commit with `git add -A`, so **stage your files explicitly,
  never `-A`**, or your work lands in their commits.
* **Make a pre-move build first** (or identify an existing one). Acceptance is
  `build_parity <pre> <post>` reporting IDENTICAL, and you cannot produce that after the fact.

---

## 1. Scope, and two corrections to the plan

**Scope is live code only.** `archive/` is old code kept for reference and is **out of scope**:
never edited, and excluded from every grep in this document. The same goes for `.venv/`,
`node_modules/`, `__pycache__/`, `graphify-out/`.

**`project_config.py` is at the REPO ROOT**, not `pipeline/project_config.py`. `config.yaml` is
beside it. Its top-level keys today are `output`, `data`, `data_accessor`, `llm`, `graph_builder` —
`curated:` becomes a sixth.

**The plan's "~60 references" over-counts the work by roughly half.** Most hits are *prose* — a
docstring saying "read from `splits.json`", a UI string saying "saved to splits.json — rebuild to
apply". Those want updating for accuracy but **cannot break anything**. The lines that actually
resolve a filesystem path are the list in §2, and there are about **40**.

**One grep lies to you:** `pipeline/hack/waterbody_splits.py:46` and `pipeline/hack/wb_present.py:12`
name `pipeline/docs/waterbody-splits.json` — a **different, generated file** that happens to end in
`splits.json`. Not in scope. Leave them.

---

## 2. The real path resolutions — the checklist

Everything below resolves a path at runtime. Everything *not* below is prose.

### `splits.json` — 12 sites

```
curation-review/backend/reuse.py:51      SPLITS_JSON_PATH = _ROOT / "pipeline" / "splits.json"
curation-review/backend/rebuild.py:42    "--splits", "pipeline/splits.json"      ← literal CLI arg
pipeline/build.py:243                    _DEFAULT_SPLITS = Path(__file__)...parent / "splits.json"
pipeline/tools/audit_split_binding.py:45     def audit(..., splits_path: str = "pipeline/splits.json")
pipeline/tools/audit_split_binding.py:111    ap.add_argument("--splits", default="pipeline/splits.json")
pipeline/dfo_salmon/dossier.py:27        SPLITS = Path(__file__)...parents[1] / "splits.json"
pipeline/dfo_salmon/match.py:53          SPLITS = Path("pipeline/splits.json")   ← cwd-dependent
pipeline/hack/audit_splits.py:55         splits_path or (ROOT / "pipeline/splits.json")
pipeline/tests/test_splits.py:146        parents[1] / "splits.json"
pipeline/tests/test_splits_accuracy.py:37,38     "pipeline/splits.json" ×2
pipeline/tests/test_anchors.py:268       load_split_defs("pipeline/splits.json")
```

`rebuild.py:42` is **the `review_build` bug repeating verbatim** — the app hands the builder a
hardcoded path string while `get_config()` sits one import away in the same file. Fix that one
first; it is the single clearest justification for the whole exercise.

`dfo_salmon/match.py:53` is `Path("pipeline/splits.json")` — relative to the **current working
directory**. It works only because everything is run from the repo root.

### `overrides.json` — 10 sites

```
curation-review/backend/reuse.py:46      OVERRIDES_PATH = _ROOT / "pipeline" / "matching" / "overrides.json"
pipeline/reach/covered.py:33             DEFAULT_OVERRIDES = parents[2] / "pipeline" / "matching" / ...
pipeline/parsing/batch_exporter.py:255   parents[1] / "matching" / "overrides.json"
pipeline/parsing/backfill_matched.py:81      default="pipeline/matching/overrides.json"
pipeline/parsing/backfill_identity.py:102    default="pipeline/matching/overrides.json"
pipeline/parsing/prune_remapped.py:74        default="pipeline/matching/overrides.json"
pipeline/hack/complex_regs_report.py:19  _OVERRIDES = "pipeline/matching/overrides.json"
pipeline/dfo_salmon/dossier.py:28        parents[1] / "matching" / "overrides.json"
pipeline/tests/test_dfo_salmon.py:1943,2022    parents[1] / "matching" / "overrides.json"
```

**This is the file with live concurrent edits** (DFO curation writes it). Sequence it late, and
land it in its own commit.

### `name_variants.json` — 5 sites

```
pipeline/build.py:426                    args.name_variants or (parent / "name_variants.json")
pipeline/hack/name_variants_dedup.py:37  _NV = _ROOT / "pipeline" / "name_variants.json"
pipeline/hack/name_variants_compile.py:214   ap.add_argument("--out", default=...)
pipeline/tests/test_added_lakes.py:112,150   ROOT / "name_variants.json"
pipeline/tests/test_dfo_salmon.py:2051   parents[1] / "name_variants.json"
```

### `areas.json` — 2 sites

```
pipeline/splits/area_splits.py:23        ROOT / "pipeline/areas.json"
pipeline/tests/test_tiles.py:96          ROOT / "pipeline/areas.json"
```

### `added_lakes.geojson` — 5 sites

```
pipeline/build.py:242                    _ADDED_LAKES_GEOJSON
pipeline/hack/added_lakes/ingest.py:20   GEOJSON = parents[2] / "added_lakes.geojson"
pipeline/tests/test_added_lakes.py:31,62,73      ROOT / "added_lakes.geojson"
```

### `added_streams.build.json` — 2 sites

```
pipeline/build.py:241                    _ADDED_STREAMS_JSON
pipeline/hack/added_streams/build_dataset.py:1346    Path(out_dir) / "added_streams.build.json"
                                         (and :1493, the out_dir default — a comment explains it
                                          resolves to pipeline/, "sibling of splits.json")
```

### `gauge_match.json` — 1 real site

```
pipeline/hydro/match.py:349              MATCH_FILE = parents[1] / "gauge_match.json"
```

`bundle/build.py:274-281` and `build.py:559-581` reach it through `read_match()`, so they need no
change. The `test_gauge_match.py` hits are all `tmp_path`. **Coordinate with the other agent
anyway** — this file is theirs right now.

### Entry directories

**Synopsis — one intended choke point, four modules bypassing it.**

```
pipeline/parsing/io.py:40                def entries_dir()   ← THE choke point
```

Used correctly by `backfill_exemptions`, `backfill_matched`, `backfill_rule_subjects`,
`backfill_identity`. **These four re-derive it instead** and must be changed too:

```
pipeline/parsing/ingest.py:207           Path(__file__).resolve().parent / "entries"
pipeline/parsing/batch_exporter.py:258   Path(__file__).resolve().parent / "entries"
pipeline/parsing/synth_responses.py:71   Path(__file__).resolve().parent / "entries"
pipeline/parsing/dispatch.py:283         Path(__file__).resolve().parent / "entries"
```

Plus, outside `pipeline/parsing/`:

```
curation-review/backend/reuse.py:39      ENTRIES_DIR = _ROOT / "pipeline" / "parsing" / "entries"
pipeline/tests/test_entryfiles_valid.py:19   parents[1] / "parsing" / "entries"
pipeline/hack/delete_stale_matches.py:25     _ENTRIES_DIR
pipeline/hack/migrate_entries.py:25          _ENTRIES_DIR
pipeline/hack/backfill_tributary_symbols.py:28   _ENTRIES_DIR
```

**Best single move here: make `io.entries_dir()` read config, convert the four bypassers to call
it, and everything downstream follows.** Roughly ten argparse `help=` strings say
`(default: pipeline/parsing/entries)` — prose, but update them or they become lies.

**DFO — two constants, one of them redundant.**

```
pipeline/dfo_salmon/entries.py:48        ENTRIES_DIR = Path(__file__).parent / "entries"   ← the real one
pipeline/dfo_salmon/dossier.py:26        ENTRIES = Path(__file__).resolve().parent / "entries"
```

`dossier.py` re-derives what `entries.py` already exports, and `splitwork.py` imports `ENTRIES`
*from dossier*. Point `entries.py` at config and have `dossier` import it — that collapses three
call sites into one.

---

## 3. Order of work

Follow the plan's six steps. The refinements that matter:

1. **Config first, pointing at CURRENT locations.** Nothing moves. Suite must be green here — if it
   is not, the accessors are wrong and you have learned it for free.
2. **Convert by area, in this order** — cheapest and most isolated first, so a mistake is small:
   `hack/` → `tests/` → `pipeline/` internals → `pipeline/build.py` → `curation-review/backend/`.
   Run the suite between areas.
3. **Then the grep gate.** All four must be empty outside `project_config.py`, `config.yaml`, and
   docs:
   ```bash
   # run under bash. X holds only --exclude-dir (no globs); --include stays quoted per line.
   X="--exclude-dir=archive --exclude-dir=.venv --exclude-dir=node_modules \
      --exclude-dir=__pycache__ --exclude-dir=graphify-out"

   grep -rn $X --include='*.py' '"pipeline/[a-z_]*\.json"' .
   grep -rn $X --include='*.py' 'parents\[[0-9]\] / "\(splits\|name_variants\|gauge_match\|added_streams\)' .
   grep -rn $X --include='*.py' '/ "entries"' pipeline/
   grep -rn $X --include='*.py' '"matching" / "overrides.json"' .
   ```
   **Baseline today: 10 / 4 / 14 / 8 = 36 hits.** That is what you are counting down. Gate 3 keeps
   **3 legitimate hits** — `test_dispatch.py:94,106` and `test_agent_parse_flow.py:96` build
   `tmp_path / "entries"` for a fixture, which is correct and must stay. So gate 3's floor is 3,
   not 0; the other three floor at 0.
4. **`git mv` + flip config values. One commit.** So one `git revert` undoes it.
5. Docs — `AGENTS.md` rule 2 names `pipeline/parsing/entries/*.json` explicitly; `pipeline/README.md`;
   the design docs that cite paths in prose; and the two handoffs
   (`HANDOFF-dfo-curation.md` §3 has a file table, `HANDOFF-data.md`).
6. Guard test.

---

## 4. The failure mode to design against

**A missed path fails at runtime with an EMPTY LOAD, not a crash.** This repo has already paid for
it once: `--splits` with no default silently dropped 376 curated cuts from three full builds, one
of which was promoted. Nothing looked wrong.

So, alongside the grep gate:

* **`load_split_defs` must raise on a missing file**, not return `[]`. Same for the overrides and
  name-variants loaders. A curated file that is absent is a bug, never an empty set.
* **The guard test should assert every path under `curated:` exists**, so a typo in `config.yaml`
  fails in CI rather than in a build.
* Prefer `get_config().splits_path` as an argparse `default=` over `default=None` plus a fallback
  inside the function — the fallback is where the second hardcoding always ends up.

---

## 5. Decide before moving

**`pipeline/ungazetted.json` has zero references in live code** — the only mention is a matcher
docstring. Either it is dead and should be deleted, or something stopped reading it and *that* is
the bug. **Ask the user** — do not relocate a file nothing loads, and do not delete curated data on
your own judgement.

---

## 6. Done when

* `pipeline/tests/` green.
* `build_parity <pre-move build> <post-move build>` → **IDENTICAL**, boundaries included. This is
  the acceptance test; the boundary line exists precisely because the `--splits` incident hid in
  the boundary count.
* The four greps in §3.3 are empty.
* The review app loads an entry from **each** set — synopsis and DFO — and resolves a reach.
  `curation-review/backend/reuse.py` is the one place the app and builder are required to agree
  (AGENTS #16: *"3,038 of 3,038 rules identical"*), and divergence there is invisible until a
  curator hits it.

## 7. House rules

* ⛔ **Never run the LLM parser** — `run_parse.sh parse`, `pipeline.parsing.dispatch`, or anything
  spawning `claude -p`. Spends the user's credits; human-only. This migration touches parsing
  paths, so the temptation to "just verify a parse still works" will arise. Hand over the command.
* **Never rewrite `entries/*.json` without a backup and a diff.** This migration moves them; it
  must not touch their contents. `git mv` only — never read-and-rewrite, which would reformat 17
  curated files invisibly.
* **Prefix shell commands with `rtk`.**
* **`graphify query` before grepping**, then `graphify update .` after the move — the graph indexes
  paths.
