# T663 — Oxbows line 459615: the valley piece below the cut is pruned (0.45 km not bound)

Status: DROPPED — owner 2026-11-11: "The oxbows missing the one is probably fine" (accepted as is)

Source: T637 build 27 verification (c88d63d26). Same on the atlas before build 27; nothing lost against live.

blk 356529638 (wsc 300-432687-459615) has no graph sections above 1,516 m. The owner's cut is at 1,967 m, so the
valley part 1,516–1,967 m is not in the graph and the Oxbows rule does not bind it. Cause: the leaf prune removes the
unnamed upper end because a name that stops at a cut point is applied after the prune (`pipeline/atlas/build.py`);
whole-line names go on before it.

Options:
- (i) accept (owner).
- (ii) make the leaf prune protect lines a cut-point name covers (`prune_leaves(..., protected=…)`); code change + an
  atlas rebuild + binding diff. The valley piece then binds; the hillside stays nameless with zone rules, like the other
  four cut lines.

No curated-only edit fixes it. Ask the owner which; (ii) rides the next atlas rebuild.
