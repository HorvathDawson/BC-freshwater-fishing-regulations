# Stocking — PREPARED, NOT FED

**Status (owner, 2026-10-22):** kept on purpose. Nothing feeds it and no build stage runs it yet; it comes back
when the FIDQ stocking data does. Do not delete it as dead code — tidy it.

What it is: the match from a FIDQ waterbody identifier (`02322SAJR`) to the lake it stocked — the identifier
first (it is FWA's own `WATERBODY_KEY_GROUP_CODE_50K`), names only as the fallback. Same three-way shape as
`pipeline/gauges` and `pipeline/bathymetry`.

| file | does | state |
|---|---|---|
| `generate/identifiers.py` | group code -> the lake nodes carrying it (a list: one code can cover several waters) | written; no caller outside `match.py` |
| `generate/match.py` | which registry item each stocked waterbody is (identifier, then name + location) | written; pinned by `pipeline/tests/test_stocking_match.py` on synthetic rows |
| `generate/audit.py` | does the 50K identifier point at the right water (shared with bathymetry) | written; run by hand |

## What it needs before it is fed

1. **A fetch.** No `data/fetch_data.py` step fetches FIDQ yet; the roster `data/source/bc_stocked_waterbodies.json`
   does not exist. The release history (what, when, how many) is weekly feed data, not bundle data.
2. **A match run and promotion** into `data/curated/stocking/` — `config.yaml` `curated.stocking.matches` is
   `null` today. `pipeline/tools/audit/check_curated.py` already lists the artifact and reports it `not built`.
3. **A bundle writer** for the tables waiting in `pipeline/deliver/bundle/schema.sql`: `stock_water` (the link)
   and `stock_code` (species / stage dictionaries). Both are empty today and named by `cov.skip` in
   `bundle/build.py`. Keep them: the app's prepared query (`app/packages/data/src/sql/queries.ts`, PREPARED, NOT FED) reads
   `stock_water`. The retired name-keyed `release` table is replaced by these.
4. **The release:** lake values ride in `lakes/1` (`pipeline/deliver/lake_facts.py`), the one landing point —
   owner 2026-10-21, "lake modes prepared, not fed".
