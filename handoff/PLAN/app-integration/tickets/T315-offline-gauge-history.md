# T315 — Offline: keep the last week of gauge readings on the phone

Status: DONE — owner phone check 2026-11-08 passed (0.6.9). Was: IN REVIEW — built + unit-tested (b1827b9d0); needs the feeds worker deployed (owner) + emulator check (owner 2026-10-31: "in offline mode we didn't get the gauge feed data … the phone didn't store the gauge data
for the last week … had no measured line").

## Work
When online, the app caches each viewed (and saved-spot / followed) gauge's recent readings (≥7 days of the feed series +
latest + forecast) on the device, refreshed with the feed; offline, Conditions charts draw the measured line from that
cache with an "as of <time>" note instead of an empty chart. Optionally prefetch the region's gauges at idle on Wi-Fi
(respect the T298 mobile-data setting). Bounded storage (LRU, a few MB). Emulator: view a gauge online, go airplane, the
chart still shows the measured line.

Depends: T254, T256. Size: S–M. Owner: no.

- Owner 2026-10-31 extension: store ALL gauges' observed values on the phone as each feed update arrives (a small delta per update), building a rolling on-device history (≥7 days) for every station — not only viewed ones — separate from the stored HYDAT history.

Owner addition 2026-10-31: keep ALL gauges' observed values as each feed update arrives (rolling ≥ 7 days, every station,
separate from stored HYDAT, bounded, old pruned); report the per-update delta size.

## Done (b1827b9d0)
- **Publisher** (`workers/feeds/src/gauges/publish.ts`): each run writes `obs.json` beside the index — every station
  written this run, its `recent` rows of the last 6 h (`OBS_WINDOW_MS`; a phone that missed a few runs still joins up).
  Test in `gauges.robust.test.ts`; parity notes it as worker-only.
- **Phone** (`data/feed/history.ts` `gaugeHistory`, file `gauge-history.json`): every station's rows for 8 days (minutes +
  values, back in the feed's own zone), merged from each station file read and from `obs.json` on every feed update
  (update service `apply`) and at launch; plus the last 24 VIEWED whole station files (forecast, daily means, reading).
  A station never opened gets daily means made from its whole kept days. Bounded ≈ 2–3 MB max. Web: session only.
- **Offline** (`httpFeed` `history` + `online`): no request is made; the session copy, else the kept week, answers —
  marked `kept`, carried to `Aged.kept`; the Conditions chart says "Offline — measured line as of <time>, saved on this
  phone" (`CONDITIONS_WORDS.keptAsOf`, testID `chart-kept-as-of`).
- Tests: `data/feed/history.test.ts` (merge/prune/zone, never-opened station, across a launch + LRU, day means, offline
  new launch → chart drawn + kept, dead line, obs sync). Mutation (no kept fallback): 2 fail.

## Delta size per feed update (measured 2026-10-30 on staging, 25 station files scaled to 438)
| window | raw | gzip |
|---|---|---|
| one run (30 min) | ~10 kB | ~4 kB |
| `obs.json` (6 h) | ~190 kB | ~25 kB |
| 24 h | ~790 kB | ~87 kB |
For scale: index.json 368 kB / 51 kB gz (percentiles only, no values); a station file ~39 kB / 7.6 kB gz.

## Left
- Owner: deploy the staging feeds worker so `obs.json` is published —
  `npx --prefix app wrangler deploy -c app/workers/feeds/wrangler.toml` (repo root). Until then only viewed stations are kept.
- Emulator (2026-10-30, `gauge-offline`): Vedder River viewed online, then airplane + cold start → the gauge's reading
  came from the phone ("4.2 m³/s … checked 37 min ago") BUT the estimate said "The live readings did not arrive": the
  feed INDEX was not kept. Fixed (f830bbf39: the index is kept and answered offline too). Rerun blocked by the
  emulator's network toggle sticking; rerun `node app/tools/testbed/run.mjs gauge-offline` (needs a `--build`).
