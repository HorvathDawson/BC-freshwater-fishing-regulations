# T367 — UI text clean-up, owner back-and-forth (regs, conditions map, More)

Status: OPEN (owner 2026-11-03; in 0.5.0, batch 6b of RELEASE-PLAN-0.5.0.md, after the small app fixes, before the
release check).

Why: the owner wants a clean-up pass mostly in the regs pane, the conditions map pane and More: badly formatted text,
and much less top-level text (debloat what shows before you tap in).

## How it runs (owner: "a back and forth item")
A loop, one round per owner note:
1. The owner says what bothers them (screen + what it should be).
2. Claude writes a short plan for that note (what changes, which file, any data-side cause).
3. Claude fixes it on the dev web app; the owner checks it there.
4. Repeat until the owner calls it done.

Only then does it go into the 0.5.0 APK (the one APK for the batch). No APK per round.

## Notes
- Text that comes from the data (rule wording, place names) is shown as sourced. A fix to its formatting is a display
  change; rewording or dropping content needs the owner's yes (ask-before-filtering rule).
- Keep a running list of the rounds here (note → fix → owner verdict).

## Rounds
- 0 (2026-11-03): regs loading spinner sat in the pane's top corner → centred, large size (0db30c2). Owner: OK.
- 1 (11-03): the gentle access note was raw grey text with a "See the map" link → its own block in the accent (not the
  amber), titled "Access may be restricted"; both map links dropped (they moved to the map without highlighting the
  area). Superseded by round 3.
- 2 (11-03): Runs tab: species chips → the spawning picture; the "made for another map" line → a visible bar on the map.
  Planned, not yet built. Cause of the stale run timing: the feed was built 10-29 for handles 10cbcb72…, the release's
  tiles are 822db448… (regs paint and conditions ship IN the release, so they always match) — owner sent it to the
  pipeline chat.
- 3 (11-03): (a) access → slim strips PINNED under the regs header, outside the scroll: amber "No public access ·
  <area>" and accent "Access may be restricted · <causes>", one line each, a tap opens the sentence. Both show at once
  (core `accessNotices` / `leadingAccessNotices`: the reserve is no longer hidden by the watershed). Banner wording
  shortened: "… is in X, closed to the public. The regulations below don't give you access." (b) forecast chips named
  by date ("Wed 4"), every day +1..+10 (T375's feed), the strip scrolls sideways (`CornerStrip scroll`); spoken labels
  unchanged. Verdict pending.
- 4 (11-04): the strip's dotted swatch read as "…" → removed. Round 2 built: Runs species chips show the spawning
  picture; a stale/failed run timing is an amber bar over the map (`PairBanner`, `col.runsProblem`, Runs tab only),
  no longer a grey line in the foot; words "Run timing is out of date for this map, so it isn't drawn." Verdict pending.
- 5 (11-04): ten dated chips pushed Change out of sight → `HorizonPicker` (map corner + panel header): Now · Forecast ▾
  (or the chosen day, filled) · Change; the middle opens the ten days under the strip, a pick closes it. The strip's
  scroll option is gone. Verdict pending.
- Open (not this chat's): `WaterScreen.regs-props.test.tsx` (fixture release, T35 Licence) ×2 and `WaterMode.test.tsx`
  (ONE MAP ENTRY) fail on HEAD without the round 3 edits too — the page shows the spinner after the keep tab appeared.

Start a UI chat from `handoff/PLAN/NEXT-SESSION-UI.md`.

## From T343 (2026-11-03, data chat → UI chat)
Two salmon-card items are yours (presentational files): (a) the COLLAPSED salmon section shows one image, the
ocean-phase chinook (`app/apps/mobile/assets/fish/chinook-salmon-ocean-phase.webp`); OPENED, each species row uses its
own `<species>-salmon-spawning-phase.webp`. (b) The DFO source info (page link, date modified, notice numbers) is one
collapsible line, collapsed by default; opened it shows the full source details. The view model is unchanged (core
`salmonCard`: `source`, `asOf`, `credit`, `dfoWords`).
(c) T343: draw the salmon card's new `hours` line (core `salmonCard(...).hours`, e.g. "Salmon fishing in daylight only:
one hour before sunrise to one hour after sunset"; null when all closed) near the gear line. Then tell the data chat (or
change core `salmon.ts` `asWords`: drop `n.fold === "hours" ||`) so the daylight note leaves the region block. Before 0.5.0.
(d) Forecast day chips: the every-day feed (+2, +4, +6–9) is the DATA chat's, T375. Build the chips on the days the feed
carries (by date); the rest fill in when T375 lands.
(e) T375 LANDED in the code (2026-11-03): `FORECAST_DAYS` is now [1..10] (core gauge-policy.generated.ts). Your ui tests
that pin the four days fail until updated with your chips: conditions.render (donorView ×2), MapScreen corner ×2, the
Change chip ×2 ("after +10" still holds: +10 is last). The live feed carries every day after the feeds worker redeploy.
(f) Forecast-day list: the centre basin, a water's own basin and the basin names are the data chat's, T376 (batch 4).
(g) Run timing (your 11-03 finding): FIXED 2026-11-04 (T373) — the local feed is bound to 822db448 again (reload the dev app); every release now binds its own.
(h) Your two asks (2026-11-04), DONE IN YOUR WORKING TREE, uncommitted — commit them with your files (they share them):
  `Shell.tsx` passes `fishImages={p.fishImages}` to `<RunsLegend>`; `shell/MapLegend.tsx` `RunsLegend` takes `fishImages`
  and hands it to `RunSpeciesChips`. The six species colours are tokens now — `color.ui.run-species.<steelhead|chinook|
  chum|coho|pink|sockeye>` (in `color.ui.*`, not `color.runs.*`: card colours, not map paint, so the map's ΔE guard does
  not compare them with the lake fill and the run-pass amber — it failed the style build as `color.runs.*`). `theme.ts`:
  `Palette.runSpecies[name]` from those tokens; `RUN_SPECIES_COLOUR` is gone; `RunsControls.tsx` + `MapLegend.tsx` read
  `palette.runSpecies[s.name]`. The tokens are committed (data chat); tsc + check-colours pass.
  Also yours, failing on your in-progress work: panel.silent (new `title`), the gauge reading's band words ("Below normal
  ·"), data-vs-ui's frozen list, back-layers, and the T375 horizon tests listed in (e).
