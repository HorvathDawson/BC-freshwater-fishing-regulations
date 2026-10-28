# T213 — paint bait-ban and fly-fishing-only waters distinctly (future)

Status: DONE — 74caddda, 898fd9e6

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
