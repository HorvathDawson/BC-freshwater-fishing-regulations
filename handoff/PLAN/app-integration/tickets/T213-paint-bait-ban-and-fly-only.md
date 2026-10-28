# T213 — paint bait-ban and fly-fishing-only waters distinctly (future)

Status: DONE — 74caddda, 898fd9e6; reworked 0c203100 (marks on Standard, switch by Layers, day strip)

Source: owner 2026-10-27. The map paints a water "has restrictions" whatever they are; bait ban and fly fishing only
(and similar method restrictions) should read differently at a glance — own colour/pattern or a symbol, in the
standard regulation map mode, legend included, cvd-safe, beside the T211 Standard/Salmon toggle. Future ticket.

## Built (2026-10-27) — DONE, 74caddda (data) + 898fd9e6 (paint)
- **Data:** release file `method` (`method/1`, `pipeline/deliver/method.py`): per section and day none / bait ban /
  fly only, a PROJECTION of the answers' open-days strips (`strip` bits 4/8/16 via the regs store) — the map and the
  water page's strip cannot disagree. A day closed at every moment is none (closures stay the status index's: carrying
  them cost 125 kB gz and crossed the 15 MB total gate); a bait ban/fly rule of some moments only is not painted.
  308 kB gz (gate 600 kB); release r-f7faa483d2b1 = 14.90 MB. 937k sections ever listed: bait 607k, fly 11k.
- **App:** `@app/core` `decodeMethod` / `methodDayProfiles`; `loader.method()`; map mode `method` on the one colouring
  engine (web + native: label `gm` per tile, day in the style, closures matched first from the status label `p`);
  toggle **Standard / Salmon / Bait & fly** in the Map tab legend (only what the release carries), legend fly only /
  bait ban / no bait or fly restriction / closed + day button. Tokens `color.method.*` (light/dark/cvd), held ≥ 20 ΔE
  apart for every reader and ≥ 15 off the ground (`tools/method-cvd.test.ts`; the status hues alone fell to 15.9).
- **Verified (web, light + dark):** `T213-shots/` — Cowichan at Skutz: fly only Oct 27 (Sep 1–Nov 15), bait ban
  May 15; Region 1 streams bait ban; Chemainus closed May 15.
- **Tests:** `pipeline/tests/test_method_file.py` (+ live: no gear class on a status-closed day), test_release (40
  passed); core `method.test.ts`, map `method-paint.test.ts`, ui `mapModes`, ui-native toggle, `method-cvd`.
- Not done: no water-level rollup (search rows keep the status colour); weekday/hour-only gear rules are not painted.

## Rework (2026-10-28) — 0c203100: marks on the Standard map, no Bait & fly mode
Owner: "should just be more styles on the regulations map — make it clean"; "the Salmon/Standard switcher is weird in
the bar — near the layer icon"; "date selector up there to match Conditions' location for selecting date offsets".
- **Encoding (chosen: ink edge).** The status colour stays the line's fill; `paint.ts` `gearPaint` draws an ink edge
  (`color.gear.edge`, the gauge-dot ink: no hue) UNDER the line — solid = fly fishing only, dashed (`dash.gear.bait`) =
  bait ban — and a ring round a lake. Four runtime layers (`GEAR_LAYERS`), stream edges over the selection casing and
  under the line, lake rings under the selection ring; shown only in the `profile` mode, off where the status day closes a
  section or marks it no access. Prototyped against breaks in the line (read as the no-access dots; bait bans blanket
  whole regions) and symbols along it (◆ / tick: legible, cluttered at z11): `T213-shots/variants/` (light/dark/cvd,
  `compare-cowichan-oct27-light.png`).
- **Removed:** the `method` colour mode (4 layers), `color.method.*`, `method_class` enum, `closed` on profile modes,
  `RegsLayer` "method", `mapModes` `method`. Kept: the `method` channel (`gm` labels) feeding the marks.
- **Legend:** one strip — the statuses, then "Fly fishing only" / "Bait ban" as hollow edge swatches (`LegendCount edge`).
- **Controls:** Standard / Salmon is `shell/RegsLayerToggle.tsx`, a compact two-row card under the Layers button
  (`MapRail` children); the day is `MapDayStrip` (Today · Tomorrow · calendar) in the top-left corner, drawn with the
  horizons' strip (`CornerStrip.tsx`), words from core `mapDayChips`. Both only while the Map tab draws regulations.
- **Verified (web :39218):** `T213-shots/gear-*.png` — Fraser Valley z11 May 15 and Cowichan z13 Oct 27 (fly + bait) /
  May 15, light, dark, cvd; selected (search highlight) light + dark; phone width. Android emulator NOT run (same
  components and style path; the phone-width web shot is the layout check).
- **Tests:** `method-paint.test.ts` (marks, gating, engine repaint, first-frame order), `tools/method-cvd.test.ts` (edge
  off own/base and every ground for every reader; mutation-checked), `dayPicker.test.ts`, `DayCalendar.test.tsx`,
  `SalmonCard.test.tsx`, `MapDay.shell.test.tsx`, layer-order / parity / profile-style / one-section-layers updated.
