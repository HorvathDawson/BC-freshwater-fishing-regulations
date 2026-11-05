# `dfo_salmon/` — DFO's salmon places and limits wording

| path | kind | written by | read by |
|---|---|---|---|
| `places/region-*.json` | curated | DFO review (marks + write-through), by hand | reach run, bundle, feeds worker (synced to R2 `private/feeds/dfo/entries/`) |
| `wording.json` | authored | `pipeline.regs.dfo_salmon.wording build`, entries checked by hand | the pipeline and the feeds worker — its ONLY copy (R2 `private/feeds/dfo/wording.json`; missing / invalid fails the DFO job) |

Both reach R2 through `python -m pipeline.deliver.sync_inputs` — automatically on push to the dev branch
(`.github/workflows/feeds-sync-inputs.yml`, staging only); production is a manual run.
