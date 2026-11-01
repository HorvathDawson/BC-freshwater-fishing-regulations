# T345 — Coquitlam River: "Lougheed bridge to watershed boundary" highlight runs past the bridge

Status: OPEN — bug (owner 2026-10-31).

On the Coquitlam River water pane, stepping along the river (upstream/downstream navigation), the highlighted stretch for
the section "Lougheed bridge to watershed boundary" extends PAST the Lougheed bridge (downstream of it), while the
adjacent downstream section correctly ends at the bridge. So the two highlights overlap below the bridge.

Find whether it is (a) the section geometry/cut — the cut at the bridge exists but this section's drawn line keeps a
piece below it (end-cut / sliver: memory end-cut-halves-the-water), or (b) the pane's highlight — it draws the section by
item/blk range rather than the section's own geometry, or rounds the measure. Compare the section's measure range with
the cut's measure and the tile geometry for both sections. Fix at the cause; add a test that adjacent sections' highlights
meet at the cut and never overlap (sample Coquitlam + a few other bridge-cut rivers).
Size: S–M.
