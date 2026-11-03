# T345 — Coquitlam River: "Lougheed bridge to watershed boundary" highlight runs past the bridge

Status: OPEN — bug, HIGH (one path crashed the app) (owner 2026-10-31).

On the Coquitlam River water pane, stepping along the river (upstream/downstream navigation), the highlighted stretch for
the section "Lougheed bridge to watershed boundary" extends PAST the Lougheed bridge (downstream of it), while the
adjacent downstream section correctly ends at the bridge. So the two highlights overlap below the bridge.

Find whether it is (a) the section geometry/cut — the cut at the bridge exists but this section's drawn line keeps a
piece below it (end-cut / sliver: memory end-cut-halves-the-water), or (b) the pane's highlight — it draws the section by
item/blk range rather than the section's own geometry, or rounds the measure. Compare the section's measure range with
the cut's measure and the tile geometry for both sections. Fix at the cause; add a test that adjacent sections' highlights
meet at the cut and never overlap (sample Coquitlam + a few other bridge-cut rivers).
Size: S–M.

## Also (owner 2026-10-31): "Show all parts" makes it worse
With **Show all parts** on, navigating to the section **"Maryhill bypass to Lougheed bridge"** highlights only a tiny
piece just below the Lougheed bridge, instead of the whole stretch from the Maryhill bypass up to the bridge. Same
boundary, likely the same cause: the part around the Lougheed cut is mis-assigned. The upstream section claims the
stretch below the bridge, so "Maryhill bypass → Lougheed" is left with a sliver. Check that both sections' parts are
whole and meet at the bridge in both views (normal and Show all parts). The test covers both.

## Also (owner 2026-10-31): "Flows into Fraser" → crash
At the Maryhill bypass section the pane says **"Flows into Fraser"**. That's right, but it flows into the TIDAL Fraser.
1. Tapping "Flows into Fraser" opens the Fraser pane.
2. Then tapping the map inside the Fraser pane **crashed the app** (once).
3. On a retry it didn't crash, but the pane showed made-up stretches: **"2 stretches · stretch 1 of 2"**, stretches the
   Fraser doesn't have at that point.

To check:
- **Find the crash.** Look in the crash report the app sent to the staging feeds worker (/crash, digest) around the
  owner's time. Reproduce with the test bed: Coquitlam → Maryhill bypass section → "Flows into" → tap the map.
- **No "Flows into" button when the receiving water is tidal (owner 2026-10-31: "we aren't doing tidal").** Show the
  line as plain text ("Flows into the tidal Fraser"), with no navigation. Where the receiver is NOT tidal, the button
  still opens that water framed at the confluence. The crash and the fake stretches still need fixing at their cause.
- **Where the fake "2 stretches" comes from.** The likely cause is the pane carrying the Coquitlam section/part state
  (the same mis-assigned Lougheed parts) into the Fraser pane, so it builds a part list from the wrong water. The
  navigation must reset part state when it switches water.
- **Regression test:** the same path — flows-into → map tap — must open the right water, never crash, and show only that
  water's real stretches.

## Evidence from T346 (staging crash store, 2026-11-02)
The crash is in the store: "Error: Requiring unknown module '…'" on route "water", platform web, two hashes
(b0eaeeb61c3d3a6c 18 reports, f1a058176d7c1a37 1), 19 in all, 2026-10-30 01:53 to 2026-10-31 07:41 UTC, versions
0.1.2, 0.2.0, 0.3.0 (all fatal). Stack: `_e.openHighlighted` → `importAll` → `Requiring unknown module`. So opening a
highlighted/linked water (flows-into) loads a module that is not in the web bundle (a dynamic/lazy require that Metro did
not bundle). Likely other waters with a flows-into link crash the same way, not only Coquitlam. Not seen on 0.4.0 yet.
Check `openHighlighted` in app/packages/ui and what it `require`s. Missing context: T352.

## Findings 2026-11-03 (code + data read; no device)
- **Overlap past the bridge: NOT in the data or the pane logic.** Cut on blk 356364584 @ 7028.62; part 2 (Mary Hill →
  Lougheed) = sids {316504, 316505}, part 0 (Lougheed → boundary) = {316506}; tiles (vintage 822db448) put 316506's start
  exactly at the bridge. Pane parts are disjoint and meet at the mark in both views (test added, waterMap.test.ts).
  Left suspects: the water-copy layer (map/src/water-copy.ts) or the Conditions gauge-route trace (GaugeTrace
  `tracePath`: bridge section → 08MH002 is exactly 316505, the "sliver"). NEXT: device repro; which tab showed it?
- **Fake "2 stretches · stretch 1 of 2": cause found.** The Coquitlam's exit is the Fraser's TIDAL part 16, which no
  map lists; `waterMapStartIx` fell back silently to index 0 (Fraser part 2, short name "2 stretches"). FIXED via the
  tidal exit below; the silent index-0 fall-back itself goes to T368.
- **Tidal exit: FIXED.** "Flows into the tidal Fraser River", plain text, no link (waterNav.ts, waterGroup.ts,
  WaterMapView.tsx + tests).
- **"Requiring unknown module" crash: GONE.** It was `import("./highlightPdf")` (b214318e9), a lazy Metro web chunk;
  today's synopsisPdf.web.ts has static imports only. Same risk class: Shell.tsx lazy `./later` (T368).

## Fixed 2026-11-03 (8a5b5bd)
- Sliver + "past its bounds": MapLibre Native's feature-state queue (per-key removals mixed with sets in one frame
  were dropped); keys now cleared by a null write. Confirmed on the emulator (t345-fix-open run).
- Web: highlight did not follow ▲/▼ (waited for a fully loaded style during the camera's flight); fixed.
- Reload onto `?regs=…&view=map` drew "Base rules" blue (the T256 colour hold never released); fixed.
- One load path (T369 for the water map): six ways into Coquitlam part 1 + each URL reloaded draw the same.
- LEFT: the 0.4.0 crash on the tapped route → close → reopen → Map: not reproduced (the emulator's map tap misses the
  river; via Open no crash). Likely the old native feature-state path; confirm on the owner's phone with 0.5.0
  (crash/2 carries the water + pane). Also: the same native queue affects whole-feature/whole-layer removals followed
  by sets (useColouring.ts clear-and-relabel, the copy's feature drop) — not changed; check in T369.
