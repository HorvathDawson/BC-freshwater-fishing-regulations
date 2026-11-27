# pipeline/stocking: PREPARED, NOT FED

Owner 2026-10-22: kept on purpose; nothing feeds it and no build stage runs it yet (it comes back when FIDQ stocking data does). Do not delete it
as dead code, tidy it. It matches a FIDQ waterbody identifier (`02322SAJR`, FWA's `WATERBODY_KEY_GROUP_CODE_50K`) to the lake it stocked: the
identifier first, names only as the fallback.

| file | does | state |
|---|---|---|
| `generate/identifiers.py` | group code to the lake nodes carrying it (a list: one code can cover several waters) | written; no caller outside `match.py` |
| `generate/match.py` | which registry item each stocked waterbody is (identifier, then name + location) | written; NOT sound under the owner's rules (it picks by nearest and falls back to names): retire it when the feed is built |

`fetch_fidq.py` and `propose.py` (PROTOTYPES, T634): FIDQ bulk preview per watershed group, lakes only, to `~/data-offgit/bc-stocking-fidq/`; code picks,
name confirms, `wbk:` stored, to `GENERATED.stocking/proposals.json`, accepted with `python -m pipeline.bathymetry.accept --set stocking`.

- Licence: the Province's stocking records are "Access Only", GoFishBC's pages "All Rights Reserved". Nothing ships until the owner decides (T634).
- To be fed it needs a fetch (no `data/fetch_data.py` step yet), an accepted match into `data/curated/stocking/` (`config.yaml`
  `curated.stocking.matches` is null), a bundle writer for `stock_water` / `stock_code` (`pipeline/deliver/bundle/schema.sql`; keep them: the app's
  prepared query reads `stock_water`) and lake values in `lakes/1` (`deliver/lake_facts.py`).
- Tests: `pipeline/tests/test_stocking_match.py` (synthetic rows); `test_doc_drift.py` keeps the `generate/` rows above real.
