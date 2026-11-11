# Bathymetry — PREPARED, NOT FED

**Status (owner, 2026-10-22):** kept on purpose. Nothing feeds it and no build stage runs it yet; it comes back
with the depth-chart feature. Do not delete it as dead code — tidy it.

What it is: a survey sheet's waterbody identifier -> the lake it is a chart of. The join is an exact key (FWA's
`WATERBODY_KEY_GROUP_CODE_50K`) confirmed by the gazette name, never by a name variant (see `__init__.py`).

No code is left in `generate/` (only `__init__.py`). `generate/audit.py` (do the two sheet sources agree, and does the
50K key land on a water whose name agrees) went with T436's dead-code sweep (971f40575); its last output,
`data/generated/bathymetry/audit.json`, is still on this Mac with no generator. Restore the audit when the depth-chart
work starts: `git show 971f40575^:pipeline/bathymetry/generate/audit.py`.

## What it needs before it is fed

1. **Sources — fetched today.** `data/source/bc_bathymetry_sheets.json` (`fetch_data.fetch_bathymetry_sheets`,
   the WFS sheets layer) and the WSA reference CSV / PDFs (`fetch_data.fetch_bathymetry_pdfs`).
2. **A matcher.** `generate/match.py` does not exist yet (stocking's `generate/match.py` is the model to copy).
   `pipeline/tools/audit/check_curated.py` lists the artifact and reports it `not built`.
3. **Promotion** into `data/curated/bathymetry/` — `config.yaml` `curated.bathymetry.matches` is `null` today.
4. **A bundle writer** for the waiting `chart` table (`pipeline/deliver/bundle/schema.sql`; empty, named by
   `cov.skip` in `bundle/build.py`). Keep it: `app/tools/bundle-schema.test.ts` requires it.
5. **The release:** depth values ride in `lakes/1` (`pipeline/deliver/lake_facts.py`), the one landing point.
