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
