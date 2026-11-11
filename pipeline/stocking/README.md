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

**Deleted:** `generate/audit.py` (does the 50K identifier point at the right water) went with T436's dead-code sweep
(971f40575). Restore it when the feed work starts: `git show 971f40575^:pipeline/stocking/generate/audit.py`.

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

## T634 first pass (2026-11-10): licence and the matcher

- **Licence.** The B.C. Data Catalogue's stocking records are "Access Only", not Open Government Licence:
  `provincial-fish-stocking-reports` (licence "Access Only", download audience "Public") and
  `fish-information-data-query` (FIDQ, "Access Only"). GoFishBC's pages are "All Rights Reserved". Nothing is built
  on stocking data until the owner decides (T634).
- **`generate/match.py` is not sound under the owner's rules** and is not to be fed: it picks the nearest of several
  lakes under one code by a point the feed does not give, counts a code hit with no name support as matched, falls back
  to "same name within 2,000 m" with substring names ("Fish Lake" inside "Little Fish Lake"), and stores node ids;
  `identifiers.py` falls back to names when the code column is missing. Retire both when the stocking feed work
  starts. Reuse instead: `pipeline.deliver.lake_facts.bind`/`core` (code first, names only confirm, `wbk:` keys) with
  the owner's stocking rules (code plus name with curated variants, never name alone, a first-time lake goes to the
  owner), and the shape of `pipeline.bathymetry.propose` / `accept` (proposals file; one frozen file only the owner's
  accept writes).
- **The original v1 chain** (`git show a40989161^:archive/pipeline/recurring/anglerinfo/`): it checked the code against
  the Province's 1:50K table, joined on the group code, and kept a hand-checked `wbid_overrides.json` (12 rows) — those
  parts were sound; its name search ran BEFORE the code join and won by being nearest with no limit, which is the
  failure the owner's rules forbid.
