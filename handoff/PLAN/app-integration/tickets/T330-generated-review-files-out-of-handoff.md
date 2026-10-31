# T330 — Generated review files out of handoff/ into data/generated/review/

Status: DONE (2026-10-30) — was OPEN (owner 2026-10-30; T324 design approved, plan in handoff/PLAN/data-flow.md §4–5).

~15 machine-written files under `handoff/PLAN/curation-review/` (tidal/*, dfo-*, entry-split-suggestions,
t89-awaiting-water, q154/sample, name-variants/candidates, proposals/) move to `data/generated/review/`, read via
`GENERATED`; review app paths updated; nothing human-made moves here. Size: S–M.

## Done (2026-10-30)
- `git mv` of the 16 machine-written files to `data/generated/review/` (same sub-paths: `tidal/{tidal-candidates,
  tidal-preverify,tidal-edits,tidal-osm-landmarks,tidal-web}.json`, `tidal/pass2/`, `dfo-{suggestions,preverify,
  split-hunt}.json`, `entry-split-suggestions.json`, `t89-awaiting-water.json`, `q154/sample.json`,
  `name-variants/candidates.json`, `proposals/`). Committed: `data/.gitignore` un-ignores `generated/review/` (the
  app's `refused-saves.jsonl` stays ignored).
- `config.yaml generated.review` + `Generated.review`; every reader / writer goes through `GENERATED.review`:
  review backend (`awaiting_water`, `dfo`, `preverify`, `tidal`), `regs/review/{names,proposals,q154,split_suggest,
  tidal}`, `regs/dfo_salmon/{edge_gaps,preverify,suggest}`, `tools/audit/tidal_{candidates,pass2,preverify,triage,
  words}`, the handoff generator scripts (`name-variants/mine.py`, `tables.py`, `q154/sample.py`), tests, ODbL notice.
- Not moved: `tidal/owner-decisions.json` (human, T326; `tidal_triage` now reads it via `proposals.TIDAL_DECISIONS`),
  the .md reports, scripts, slough files (T322). Writers that also emit a .md summary now write it beside the json.
- Checked: the live backend (:8787, reloaded on its own) serves tidal / DFO / Q154 progress, queues and DFO
  suggestions from the new paths. Tests: tidal_triage, licence_records, curation_review_{tidal,tidal_unregistered,
  q154,screen}, dfo_suggest, name_sources_t224, review_proposals, review_tidal_proposals — pass.
