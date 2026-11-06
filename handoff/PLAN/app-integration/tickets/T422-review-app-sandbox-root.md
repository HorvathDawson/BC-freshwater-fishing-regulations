# T422 — The review app's sandbox: one temp root instead of four path overrides

Status: DONE 2026-11-05 (see Outcome). Was: OPEN (leftover of T331 item 2, split out 2026-11-05).

`CURATION_MARKS`, `CURATION_SPLITS_JSON`, `CURATION_ENTRIES_DIR`, `CURATION_DFO_DIR` (curation-review/backend `reuse.py`,
`dfo.py`) still repoint curated files one by one. They are the review app's sandbox: ~15 test files start the app with
them, and `regs/review/parity.py` / `gate.py` set `CURATION_ENTRIES_DIR` / `CURATION_BUILD` for side builds. A path
override per file is how a test or side build can read a mix of real and sandbox files without noticing.

## Work
One mechanism: the backend takes a root (e.g. `--config <file>` or `CURATION_ROOT`) and every curated path resolves
under it (`pipeline.common.curated`); tests and side builds point at a temp root; the four overrides are removed and
refused.

## Acceptance
No `CURATION_*` path override left (a grep test); the ~15 review-app tests and the parity/gate side builds run on a
temp root.

Size: M. Owner: no (the design call is Claude's, unless it changes how the owner starts the app).

## Outcome (2026-11-05)
**Design (Claude's call; how the owner starts the app is unchanged — `bash curation-review/run.sh`, `PORT` /
`CURATION_VITE_PORT`).** `CURATED_ROOT=<dir>` moves the WHOLE curated tree: `pipeline.common.curated.load()` rebases
every `curated:` path from `data/curated` to `<dir>` (same layout; a path outside `curated.base` is refused), cached per
root, so `CURATED.*` follows it at access time. `make_root(dst)` copies the live tree (15 MB, a copy never links;
refuses a dst overlapping the live tree); `load_repo()` is the live tree for guards. Set at all, `CURATION_MARKS`,
`CURATION_SPLITS_JSON`, `CURATION_ENTRIES_DIR`, `CURATION_DFO_DIR` (and the T331-retired `CURATION_INSEASON_MATCHES`,
`CURATION_SLOUGH_PAGE_MARKS`) make any curated access RAISE (`RETIRED_ENV`). A sandbox's write-through is off by
default (`write_through.from_env` compares against the live store), as a copied store was.
`CURATION_BUILD` (and REACH_DIR / BUNDLE / RELEASE_ROOT / TILES / BACKUP_DIR) stay: generated outputs, not curated.

- Backend: `reuse.py` ENTRIES_DIR / SPLITS_JSON_PATH / MARKS_PATH / `_dfo_dir()` read `CURATED` only; `dfo.DFO_DIR`
  removed (`reuse._dfo_dir()`).
- Side builds: `parity --root` (was `--entries`), `gate --root` (was `--marks/--entries/--splits`);
  `proposals.write_side` writes a whole root at `<side>/curated` and `side_commands` print
  `CURATED_ROOT=<side>/curated` / `--splits <side>/curated/waters/cuts.json` / `--entries <side>/curated/regulations/synopsis`.
- Tests: `conftest.curated_root(mp, dst)` / `sandbox_tree(dst)` (fresh review: empty marks, no closed list); 18 test
  files moved onto it (api, browser, inseason, q154, queue, reports, safety, screen, slough, tidal, tidal_followups,
  tidal_unregistered, vintage, writes, name_sources_t224, review_proposals, review_write_through; split_preview comment).
- Gate: `pipeline/tests/test_curated_root.py` — no code names a retired override; no statement reads a `CURATION_*`
  variable beside `CURATED` (AST; mutation-checked against the old reuse.py: flags 3); refusal, rebase, bad roots.
- Docs: curation-review README / API.md, config.yaml comment, curated.py docstring.

Tests run singly (heavy job held the machine; no full suite): every touched file with CI's filter, and unfiltered:
all pass — curated_root 11, api 43, browser 2 (`-m slow`, own free ports), inseason 13, q154 1, queue 6, reports 5,
safety 32, screen 17, slough 3, tidal 2, tidal_followups 2, tidal_unregistered 2, vintage 3, writes 9, split_preview 6,
name_sources_t224 10, review_proposals 11, review_write_through 12, review_gate 29. Not run: the parity / gate CLIs
against a real side root (heavy; `--root` replaces their old flags), the full suite (one check at merge).
