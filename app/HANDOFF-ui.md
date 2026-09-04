# Handoff — the app

Written 2026-09-02, at the end of a long session. Status, what was learned, and what is
still open. Nothing here is a plan.

---

## Where things stand

325 tests, `pnpm check` green (boundaries → platform → style → spinner → fixture → deps →
typecheck → test).

**Conditions.** One implementation: `ConditionsPanel`, rendered by `ConditionsScreen` and
reached one way — by tapping a reach on the Conditions tab. The rules sheet does not render
conditions at all; its `FaceBar` toggle navigates. Both screens show the same header row
(back link, then the two face buttons) in the same place.

The panel carries: the reading and its standing, quantity and span controls (flow/level,
72 h/this year), a forecast-model picker when more than one BC River Forecast Centre run is
live, the hydrograph with envelope + prior years + a TODAY rule, the model's disclaimer
verbatim, and the route panel with a mini-map showing both ends of the trace.

**Map.** Conditions colours by Flow, Level or Both. Gauge dots carry pill labels on the same
ramp as the water; below z7 the water fades and each station becomes a soft disc. Route
reaches paint in `color.highlight` with downstream arrowheads.

---

## What was learned

**Assertions on expressions we build prove nothing about what the renderer accepts.** A
`line-width` shipped as a `case` over two zoom curves. MapLibre allows one zoom-based
`interpolate` per expression and requires it outermost, so the layer failed to *parse* — the
map came up unstyled with a single console line, and every test passed.
`tools/style-valid.test.ts` now runs the spec's own validator over the composed style
(basemap + generated layers + runtime layers + per-mode per-theme paint) for every theme and
mode.

**An effect declared above the one that creates the map never runs.** The gauge label's pill
was registered in an effect whose only dependency was the theme; on first render
`map.current` was null, it returned, and never fired again. MapLibre answers a missing
`icon-image` by drawing the text and logging, so labels rendered bare and nothing looked
broken.

**Feature-state that nothing reads is a no-op that looks like a feature.** `highlight()` had
always written `{selected: true}`; no expression consumed it, so every route highlight set a
flag into the void.

**`once("load")` on an already-loaded map never fires.** Gauge dots appeared only after
opening a reach and coming back — the remount was the only path that baked the data into the
style.

**A ramp for a map may not contain the ground's own colour.** The flow ramp's midpoint was a
sandy beige one shade off the basemap paper, so the middle of the scale — where most rivers
sit most of the time — was invisible over land.

**Sharing a component is not the same as having one surface.** The sheet's conditions face
and the Conditions tab shared `ConditionsPanel` and still diverged: only one of them can
carry the reach you tapped, the coordinate behind "you are here", and the map's quantity.

---

## Open

**~~Nothing in this session was verified visually by me.~~ — 2026-09-03: it has been.** The
app was run against the province bundle, the live feed and the full atlas, and walked with
Playwright. Seven defects, none of which any of the 323 passing tests saw; all fixed, and
the walk is written up in the *What the Browser Said* artifact. What is still unseen: the
native app. Everything here was verified through react-native-web in a desktop browser, so
gesture feel, font metrics and the native map SDK remain unproven.

**Riffle parity: audited 2026-09-03, two gaps found and closed.** The date pill was inert —
`MapScreen` declared `onDate` and `Shell` never passed one — so the app showed the qualifier
on every seasonal answer with no way to change it; `DateSheet` is riffle's "Which day?"
month grid and day stepper. And the legend carried no counts, though `LegendCount` had an
`n` prop since it was written, with a comment reading "the count is the point". Both are in.

Riffle's remaining differences are deliberate and should stay: it says "limited" where the
app says "restricted" (core's `statusWord` is the one vocabulary, and riffle predates it),
and it puts a satellite toggle on the map where the app keeps that choice in the Layers
sheet beside the other things that change what is drawn.

**The fixture bundle is still load-bearing for tests.** The app itself reads the province
bundle, but `tools/bundle-schema.test.ts`, `packages/data/src/bundle/source.test.ts` and
`tools/feed-contract.test.ts` run against `packages/data/dev/bundle.sqlite`. Deleting it
would make the suite depend on a 44 MB uncommitted build artifact. Removing it was asked for
and not done.

**Map → Conditions flash on tab switch.** A fresh mount is clean; switching tabs shows a
frame of the wrong colouring.

**Desktop is scaffolding.** `apps/web/src/desktop/` exists; the DOM component set does not.

**Contract gaps.** `app/design/data-contract.html` still names things the source cannot
answer: `entry`, `rule`, `rule_section`, `chart`, `release`/`stock_water`, `edition`,
`base_reg`, `rule_unplaced`, `tombstone`. Those are bundle-side (see the data handoff).

**The seasonal chart's prior years come from HYDAT** and are therefore two complete years
behind; the current year's line is accumulated by the feed and starts a month long. Both are
captioned, but a reader in January sees very little.

**Gauge representativeness is unsettled** and it is the app's problem too: the panel says
"no gauge is entitled to speak for this water" from a hard rule that is known to be blunt.
See the data handoff.
