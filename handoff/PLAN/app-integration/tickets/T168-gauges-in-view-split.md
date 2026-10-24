# T168 — "N gauges in view": split the gauges on this water from the donor gauges

Status: DONE — lane-T168, c54ba532 (code + tests); this ticket update follows it. App only; no rebuild, no deliver: every fact needed was already in the bundle
and the release (`gauge.item_id`, `gauge.sid`, `item_section`, `water_continue`).

Source: owner 2026-10-23: "when it says (13 gauges in view) we need to make a division between real gauges and donor
gauges." Seen on Nicomen Slough: 13 = its own 2 gauges + 11 Fraser gauges lending an estimate. Scope added 2026-10-24
(owner): "nicomen slough has so many not reporting gauges. they need to be hidden under drop down."

## Work
The count and the list say which gauges stand ON this water (its own gauges: same item / own water, `same_river` with
the water's own line) and which are DONORS (other waters' gauges the estimate borrows from), e.g. "2 gauges on this water
· 11 estimating from other waters". Panel rows, the gauge switcher and the map's gauge chips group the same way (own
first). One function decides own vs donor (core), used by every surface; whole-value tests; shots (Nicomen Slough,
Chilliwack, a lake with its own gauge) light/dark, phone/desktop. Ships: app (and the bundle if a field is missing).
Size: S. Added: within each group, the gauges NOT REPORTING (T123 `donorState` not `reads`) fold under one closed
disclosure ("6 not reporting ▾"); the count line counts them apart.

## As built
- **The one decision — core `gaugeSide`** (`app/packages/core/src/gaugeSide.ts`). A station is ON THIS WATER when its
  `gauge.item_id` is one of the section's waters: the section's own item(s) plus every water joined to one across
  `water_continue`, both ways and transitively — one system, exactly as `build_panels.own_distances` reads a water's own
  gauges (the Chilliwack's 08MH001 is the Vedder's own). A station on no named water (296 have no item) is own only on
  the very section it stands on. Everything else is a DONOR. Not `panel_member.lead` (Nicomen's panel 4739 gives the
  Fraser gauges lead 3–8) and not `same_river` (true of every Fraser gauge there) — neither says whose gauge it is.
- **The one grouping — core `groupGauges`**: own first, then donors; in each group the reporting members (`donorState`
  `reads`, i.e. `donorCounted`) in sight and the rest folded, with the fold's words (`"6 not reporting"`, screen-reader
  `"6 gauges from other waters not reporting. Show them"` / `"… Hide them"`). The fold word follows the state and the
  horizon: "not reporting or not ranked" where an unranked-but-reporting member is among them (never calling a reporting
  gauge silent), "not counted this far ahead" on +1…+10, "with no forecast running" under Change.
- **The count line — core `gaugeCountLine`**: reporting by side, the rest once, so the parts add up to the panel:
  Nicomen today "No gauge on this water reporting · 5 estimating from other waters · 8 not reporting or not ranked";
  Chilliwack "3 gauges on this water · 1 estimating from another water · 5 not reporting"; Vedder (continuation) "3
  gauges on this water · 1 estimating from another water · 6 not reporting"; Harrison Lake "1 gauge on this water".
- **Data** (`@app/data`): `RegsSource.gaugeWaters(section, stations)` → `{ waters, gauges: station → {item, section} }`,
  in the bundle reader (`ITEMS_OF_SECTION`, `continueLinks`, `itemIdsForOrds`, `gaugeWatersFor`) and the release reader
  (core `ITEMS_FOR_SECTIONS`/`ORDS_FOR_ITEMS`/`ITEMS_FOR_ORDS`, water `CONTINUE_LINKS`, conditions `GAUGE_WATERS`), the
  closure shared (`bundle/shared.ts` `continuedOrds`, `gaugeWatersOf`). A cut without `water_continue` reads each water
  as its own system. The fixture answers none.
- **UI** (`@app/ui` `panel.ts`): `useGaugeSides` (stations = the panel's members, or a lake's own station) feeds
  `useConditionsView` (`sides`, `count`, `gaugeGroups` = `gaugeChoiceGroups`) and `donorView` (`groups` for the rows —
  EVERY member, the silent ones now rows inside their fold; `map.keyGroups` and own-first `keys`/pin `rank`). Without
  sides (a saved spot, or while reading) nothing groups and every surface is as before.
- **Surfaces** (`@app/ui-native`): one `GaugeGroups` component (heading, shown items, the closed fold with
  `aria-expanded` and the count in its label, 44 px press) draws the donor rows (`DonorPanel`), the chart's gauge switch
  (`ConditionsPanel` `GaugeSwitch`; the charted gauge stays in sight even when quiet) and the route map's key
  (`DonorMap`; the key never folds — each key names a pin drawn on the map). The count line heads the gauge column
  (`gauge-count`), and under the reason on a lake / no-estimate. With groups, the old "Not reporting: …" line is not
  drawn (its members are the folds' rows).

## Tests (whole values, mutation-pinned)
- `core/src/gaugeSide.test.ts` — Nicomen's 13 stations (bundle facts) split 2/11; continuation; item-less gauges;
  Nicomen's groups whole; `keep`; horizon words; the count line in every shape. Mutations M1–M5 (own for any placed
  gauge; no section rule; fold on `quiet` only; no unranked word; quiet own counted as on this water) each fail.
- `ui/src/panel.sides.test.ts` — rows grouped with folded rows' own words; no sides → unchanged; key own first and never
  folded; switch keeps the charted quiet gauge. M6–M8 fail it.
- `tools/gauge-waters.test.ts` — in-memory bundle (node:sqlite, so in tools/): Vedder ↔ Chilliwack ↔ Sumas both ways and two links deep;
  no `water_continue`; no section. M9 (one link direction), M10 (one hop) fail it.
- `ui-native/src/DonorPanel.groups.test.tsx` — Nicomen-shaped panel: count line, own above donors, fold closed by
  default, `aria-label` "2 gauges on this water not reporting. Show them" → open → "… Hide them", the rows appear, folded
  dot quiet grey. M11 (open by default), M12 (`quietAt` from `rows` only) fail it.
- `ui-native/src/ConditionsPanel.groups.test.tsx` — wired through `useConditionsView` (count, switch, rows) and a gauged
  lake. M15 (no `gaugeWaters` call) fails it.
- `tools/one-gauge-side.test.ts` — NO SECOND COPY: outside core `gaugeSide.ts` no module tests `.waters.includes(…)`,
  compares a side to "own"/"donor", or spells the group words; every pattern proven to fire; panel.ts calls
  `groupGauges` for rows, key and switch and all three ui-native surfaces draw `<GaugeGroups>`. M13 (the own-first sort
  first written in ui), M14 (a heading literal in GaugeGroups) fail it.
- `REGS_STORE_REQUIRED=1 pnpm -C app check` via `scripts/heavy.sh appcheck 30`: green, 8,117 passed / 24 skipped (2026-10-24, before c54ba532). The full mutation list (M1–M15) was run with `scratchpad/T168-mutate.py`; each failed its test and was reverted.

## Shots (own Expo port 8197, release data from :39217)
In `/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/.playwright-mcp/T168/`:
`nicomen-desktop-light.png` (count line, switch and rows grouped, folds closed), `nicomen-desktop-light-own-open.png`
(own fold opened: 08MH085/086 rows, "not counted today"), `nicomen-phone-dark-switch.png`, `nicomen-phone-dark-count.png`,
`chilliwack-desktop-dark.png`, `chilliwack-phone-light.png`, `vedder-continuation-desktop-light.png` (08MH001 on this
water for the Vedder), `harrison-lake-phone-light.png`, `harrison-lake-desktop-dark.png` ("1 gauge on this water");
before: `before-nicomen-desktop.png`.

## Adversarial review
- **Polygon section**: Nicomen's lake polygon 1265716 and connector 59620 are `item_section` rows of gnis:21105, so
  their panel (3654, own only) groups as on this water; panel 280's Nicomen sections (Fraser only) read "No gauge on
  this water · …" — true of that panel, which T169 replaces. A polygon of no item has no waters: every gauge a donor
  unless it stands on that very section.
- **Continuation**: the Vedder's 08MH001 (on the Chilliwack) groups as on this water, on the release (shot) and in the
  bundle reader (test, transitively). Q1 below.
- **Lake gauge**: a gauged lake's station comes through the lake road (`lake_gauge.item_id` = the lake), and all 173
  lake_gauge rows have `gauge.item_id` = that lake (checked on the live bundle), so it is always own.
- **`gauge.item_id` vs `gauge.sid`**: on the live bundle every one of the 2,028 matched stations' `item_id` is the item
  of its `sid` (0 disagree), so testing the item is the same as testing the reach's water.
- **Unranked is not silent**: the first fold word called "reporting, not ranked" gauges "not reporting"; the word now
  says "not reporting or not ranked" when one is among them, and "No gauge on this water ranked" for an own one.
- **Key vs pins**: folding the map key hid keys whose pins were still drawn; the key never folds now (test).
- **A quiet charted gauge**: picking a quiet gauge in the switch would have folded the chip being charted; it is kept.

## Owner questions (defaults built)
1. **Continuation = on this water?** Default: yes (one system, as the panel's own-gauge lead reads it) — the Vedder shows
   the Chilliwack's 08MH001 under ON THIS WATER. Alternative: only the same named water.
2. **Unranked gauges fold with the silent ones** (coordinator's brief), worded "not reporting or not ranked". Default as
   built. Alternative: keep "reporting, not ranked" gauges in sight.
3. **A lake with several stations** (Shuswap has six): the sheet reads one (the lowest id, the map's choice), so the count
   says "1 gauge on this water". Default: count the station that speaks. Alternative: name every lake station.
4. **"N gauges in view" on the Conditions MAP legend** (`Chrome.tsx` `LegendRamp`) counts the visible SECTIONS with a
   standing, not gauges — likely the "13 in view" the owner saw. Untouched here (out of scope). Default: leave; a
   follow-up could relabel it ("13 reaches in view") or count distinct speaking stations.
