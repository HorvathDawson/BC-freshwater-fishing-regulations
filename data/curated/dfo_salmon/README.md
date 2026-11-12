# `dfo_salmon/` — DFO's salmon places and limits wording

| path | kind | written by | read by |
|---|---|---|---|
| `places/region-*.json` | curated | DFO review (marks + write-through), by hand | reach run, bundle, feeds worker (synced to R2 `private/feeds/dfo/entries/`) |
| `wording.json` | authored | `pipeline.regs.dfo_salmon.wording build`, entries checked by hand | the pipeline and the feeds worker — its ONLY copy (R2 `private/feeds/dfo/wording.json`; missing / invalid fails the DFO job) |
| `scope_places.json` | authored | by hand (T679 #2: was code in `pipeline.regs.dfo_salmon.entries`) | `entries` `bind-scopes` / `page-defaults` (they write region files), the atlas promote guards (`curated_boundary_ids`, `orphaned_ids`) |
| `tidal_calls.json` | authored | the owner, by hand (a place + its stated reason) | reach run (T632: every other place has its tidal water subtracted) |

`places/` and `wording.json` reach R2 through `python -m pipeline.deliver.sync_inputs`, from the Mac only (T520, owner 2026-11-08):
`deploy/staging/sync-inputs.sh`, which the review app runs by itself after a DFO save (staging only); production is
the owner's `deploy/prod/sync-inputs.sh` (CONFIRM_PROD).
