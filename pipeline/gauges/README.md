# pipeline/gauges: which water can be told its flow, and how honestly

Three halves, split by what each needs: `generate/` (needs the atlas; runs a few times a year by hand; writes candidates a
human promotes), `consume/` (needs only the frozen matches and the flow graph; feeds the bundle's gauge tables) and
`feed/` (needs only station ids; the live feed and the climatology).

- Entry: `feed/climatology.py` (the `clim` build stage: HYDAT to `clim.json`, the gauge envelope), `feed/publish.py` (the
  live feed; its TypeScript twin is `app/workers/feeds/src/gauges/`), `generate/match.py` + `generate/promote.py` (matching), and
  `scripts/dev-build.sh` / `scripts/update-hydat.sh` for the feeds.
- Reads: HYDAT and the station roster under `data/source/`, the accepted datum changes
  (`gauges/datum_change_stations.json`, `feed/datum.py`), the atlas (generate only).
- Writes: `data/generated/gauges/feeds/clim.json`, the bundle's gauge tables (via `consume/`), feed files under R2 `feeds/live/`.
- The frozen match is ONE file, `data/curated/gauges/matches.json` (coordinates + wsc/wbk); the build and the bundle both read it,
  neither matches. Promote station by station: a rerun of match + promote demoted 25 stations once (AGENTS 38).
- Representativeness (whether a gauge may speak for a water) is policy, `consume/` and `app/packages/core/src/gauge-policy.generated.ts`
  (written by `pipeline.tools.emit.gauge_policy`).

Datum changes (a gauge's zero moved): [`pipeline/docs/GAUGE-DATUM-CHANGES.md`](../docs/GAUGE-DATUM-CHANGES.md).

## Tests

`pipeline/tests/test_gauge*.py`, `test_clim*.py`, `test_feed_*.py`; the worker parity in `app/workers/feeds/parity/gauges/`.
