# Handoff — the data layer

Written 2026-09-02, at the end of a long session. Status, what was learned, and what is
still open. Nothing here is a plan.

---

## Where things stand

**Gauges.** Hydrometric stations are matched to waters once, frozen in
`data/curated/gauges/matches.json`, and read by two consumers that match nothing themselves:

```
data/source/bc_hydrometric_stations.json          fetched roster
  │   python -m pipeline.gauges.generate.match --build <a completed build>
  ▼
data/curated/gauges/matches.json                  coord + wsc (streams) · coord + wbk (lakes)
  ├──► pipeline.atlas.build     a `gauge` point anchor at the coord, scoped by wsc, appended
  │                       after splits.json and resolved by the same resolver
  └──► pipeline.deliver.bundle    the section that begins at that coord and runs upstream
```

Two-pass because matching compares a station's name against every name a node carries, and
those include `name_variants.json`, applied *during* a build. 2,097 of 2,324 stations
matched; 438 of 439 *transmitting* ones (99.8%).

**Feed.** `pipeline.gauges.feed.publish` every 30 minutes: ECCC observations, percentiles against
a HYDAT envelope, this year's daily record accumulated by the feed itself, and BC River
Forecast Centre runs (all models per station, series fetched only when the issue time
moves). `index.json` is ~70 KB and carries both a discharge and a level percentile.

**Bundle.** 44.7 MB. `section_gauge` 119,339 rows, `lake_gauge` 220, `gauge_clim` 146,564.

---

## What was learned

**A symmetric ratio cannot answer a directional question.** Trust was
`min(mag)/max(mag)`, so SLESSE CREEK NEAR VEDDER CROSSING scored `fair` for 13 reaches of
the Chilliwack off a creek carrying a seventh of the river. "The creek is 14% of the river"
reads as moderate; the honest reading is that 86% of the water is unaccounted for.

**Node ids are build output.** `{blk}:{down_m}` moves whenever the sectionizer cuts
differently — which the gauge cuts do, deliberately, on the very next build. Anything frozen
against them is stale by construction.

**Deriving twice beats freezing a derivative.** A route measure survives re-sectioning, but
it is a number we compute; the coordinate is the number ECCC publishes.

**417,420 of 721,353 lake nodes carry no watershed code.** Testing drainage during the shed
walk (rather than at claim time) would have severed every river at its first such lake. The
rebuild caught it; no test would have.

**The build hides its own regressions.** `lake_gauge` went from 220 rows to 0 when lake
stations lost their branch, and the build reported success. Row counts per table are the
only thing that showed it.

---

## Open

**Gauge representativeness is a policy, not a law.** Right now a hard drainage gate: a reach
qualifies only if its watershed code is the gauge's or descends from it. That fixed Slesse,
but it is blunt — a gauge at the mouth of a tributary carrying 90% of the flow is refused
for the mainstem just below it, and a reader standing a few metres above a gauge on another
branch may be better served by it than by anything on their own. `section_gauge` stores one
station per section with a band and two magnitudes; it does not record the *relationship*
(which direction, how far along the channel, same watercourse or not), so no alternative
policy can be tried without a schema change.

**No build has run since the gauge cuts were written.** `data/curated/gauges/matches.json` exists
and resolves (403 defs → 440 cuts against the current chains, 20 on the Fraser mainstem),
but no graph has been built with them. Sequence is build → bundle → tiles.

**Tiles are a build behind the bundle.** 339 of 118,331 gauged sections had no tile feature
at the last measurement, including 16 of the Fraser mainstem's 20; the one that survives
carries the *old* whole-river geometry, so the Fraser paints as one feature at Mission's
percentile. See `memory/tiles-bundle-same-vintage.md` for the check that measures it.

**227 stations do not match** (226 unresolved, 1 deliberate `no_match`). Unreviewed.

**Bathymetry, item 6.** One sheet covering several lakes, and 51 multi-water identifiers,
deferred by explicit instruction. The audits are on disk. Guidance given at the time: the
`(#2*)` in a Shoal Lakes title *is* the disambiguator; Cameron has a sheet per lake so both
should match; `1 LORNE LAKE 2 ISSITZ L TROUT 3 WOLFE LAKE` should map to all three.

**Name variants flagged and unverified.** `og lake` → FWA `oglake` (missing space) needs its
display name and variant fixed. Cripple → Nendatoo confirmed a real rename. Cameron/Shoal
plurals may already be fixed in `name_variants.json` — the audit reads the *built* graph, so
it needs re-running after a full build.

**Bundle tables not wired:** `entry`, `rule`, `rule_section` (need `pipeline.regs.parsing.io` and
`pipeline.atlas.reach.covered` over the full corpus), `chart` (needs the bathymetry contour fetch),
`stock_water` / `stock_code` (need the FIDQ fetch — no waterbody roster on disk).

**A parse run was stopped on credits.** `output/parse.credit-stopped-2026-09-01/`.

**The HYDAT release check** prints a warning when a newer release is out. It has never fired
in anger, so the notification path is unverified.
