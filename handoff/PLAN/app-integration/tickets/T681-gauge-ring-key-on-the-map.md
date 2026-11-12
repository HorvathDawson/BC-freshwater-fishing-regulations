# T681 — The gauge-reporting key on the map, not in the bottom bar

Status: DONE (d84c664e1)

Source: owner 2026-11-12, verbatim: "the condition gauge reporting legend should be on the map insatead of at the bottom bar".

## Work
- The Conditions key's one gauge-reporting line (T636: the grey dot with a ring, "Gauge reports level only" under Flow,
  "… flow only" under Level; only while the gauge dots are drawn, never under Temperature) leaves the bottom bar and is a
  card over the map: bottom-left, above the foot block, square corners, app tokens, no tap taken.
- Overrides, for this one line only, T367 "legend + its control in the bottom bar". The ramp, the horizon days, the Map tab's
  status key, Salmon's and Runs' stay in the bar.
- One load path: drawn by the Shell from the held look (`HeldKey`), like the bar legend, so it is the same whatever way the
  Conditions tab is entered.

Depends: none. Size: S.

## Done
- `GaugeOnlyKey` (shell/MapLegend.tsx), rendered in the Shell's foot beside `RunsKey`; `MapLegend` no longer draws the ring.
- Test: `T636.legend.test.tsx` (card position/corners; absent from the bar; Flow/Level/Temperature/gauges-off cases).
