# T331 — Curated config hygiene (small items from data-flow.md §5)

Status: PARTIAL (2026-10-30, branch cleanup/t327-t331) — 1, 3, 4, 5, 7 done; 2 done for the two named overrides,
the review-app sandbox overrides left (below); 6 waits for T325's closed list; 8 for after the batch. Was OPEN (owner
2026-10-30; T324 design approved, plan in handoff/PLAN/data-flow.md §4–5).

1. Declare every curated file in config (`dfo_salmon/wording.json`, sloughs page-marks, `sub_lake_areas.json`);
   `wording.py`/`slough.py` read through `CURATED`.
2. Drop `CURATION_*` env overrides that repoint a curated file (`CURATION_INSEASON_MATCHES`; tests use temp roots).
3. One sync list: `feeds-sync-inputs.yml` and `sync-inputs.sh` read the same key list (workflow lacks wording.json).
4. Dead files: delete or declare `regulations/cross_references.json` (test only), `waters/sub_lake_areas.json`.
5. Source data out of curated: `gauges/waterbody_type.json`, `runtiming/timing.json` → `data/source/`.
6. Close the 7 retired `proposal`-scope marks (closed list, T325).
7. Narrow the deliver key to the curated files deliver reads.
8. Prune `data/release/` (27 folders) to live + previous.
Size: M (each S).

## Done (2026-10-30)
1. **DONE.** `curated.dfo_salmon.wording`, `curated.sloughs.page_marks`, `curated.waters.sub_lake_areas` (config.yaml +
   `DfoSalmon` / `Sloughs` models, `Waters.sub_lake_areas`); `wording.REGISTRY` and `slough.PAGE_MARKS` are the
   CURATED paths. Every file under data/curated is now declared except `README.md`.
2. **DONE for `CURATION_INSEASON_MATCHES` and `CURATION_SLOUGH_PAGE_MARKS`** (both removed; their tests monkeypatch
   `inseason.matches.default_path` / `slough.PAGE_MARKS` at a temp copy). **LEFT (needs a design call):**
   `CURATION_MARKS`, `CURATION_SPLITS_JSON`, `CURATION_ENTRIES_DIR`, `CURATION_DFO_DIR` (curation-review/backend
   `reuse.py`, `dfo.py`) still repoint curated paths. They are the review app's sandbox: ~15 test files start the app
   with them, and `regs/review/parity.py` / `gate.py` set `CURATION_ENTRIES_DIR` / `CURATION_BUILD` for side
   builds. Replacing them needs a temp-root mechanism (e.g. a `--config` the backend takes) — a ticket of its own.
3. **DONE.** `pipeline/deliver/sync_inputs.py` is the one list (dfo-entries, wording, roster, inseason-matches): it
   stages each at `<out>/<R2 key>` and prints `<key>\t<cache-control>`. The workflow and `deploy/staging/sync-inputs.sh`
   only put what it staged; the workflow's `what` choices are `all` + the list (it now syncs wording.json). Roster
   bytes checked identical to the old `jq -c` output. Test: `test_sync_inputs.py`. Not run against R2 (staging run is
   the owner's / next deploy's: `deploy/staging/sync-inputs.sh --dry-run`).
4. **DONE.** `regulations/cross_references.json` deleted (no reader at all — the test only named it in a comment;
   its pointers live in the catalogue's `see` now). `waters/sub_lake_areas.json` declared (worklist, like
   `ungazetted.json`).
5. **DONE.** `git mv` to `data/source/bc_station_waterbody_type.json` (the name `check_curated` already fell back to)
   and `data/source/runtiming/timing.json`; config keys `gauges.waterbody_type` / `runtiming.timing` and their model
   fields removed. Readers: `gauges.generate.waterbody_type.TYPES_FILE` (promote, check_curated), `runtiming.parse
   --out` default, `test_runtiming_parse`; docs (runtiming README/PLAN, data/.gitignore, AGENTS 35 example).
   Which run-timing parse the app trusts: the **workflow's** (feeds-runtiming parses in its runner and builds the
   feed from that); the Mac `timing.json` is read by no stage, only the test.
6. **TODO (blocked on T325).** The closed-domains list (`pipeline.regs.review.closed`, `closed.json`) is not on this
   branch yet (T325 lane, main checkout). When it lands: add a `proposal` entry (scopes `["proposal"]`, final null,
   why "T331: the 7 retired proposal-scope marks from the old accept step; exported nowhere") — never edit marks.jsonl.
   `grep -c '"scope": *"proposal"' marks.jsonl` = 7 today.
7. **DONE.** `build.curated_digest` (the deliver key's `curated`) hashes only the declared paths, no longer `base`
   (the whole tree), minus `NOT_DELIVER_INPUTS`: proposal_overrides, review.closed, gauges.candidates, sloughs
   page marks, waters.name_calls, ungazetted, sub_lake_areas (no stage reads them). Test:
   `test_the_deliver_key_reads_only_what_deliver_can_read`. **Unproven without a build:** that deliver reads no
   undeclared curated file (grep says only review / audit tools read literal curated paths). The key changes, so the
   next `pipeline build` reruns deliver once.
8. **SKIPPED in the worktree** (a batch rebuild is reading the main checkout); the owner's lane prunes
   `data/release/` to live + previous in the main checkout after the batch promotes.
