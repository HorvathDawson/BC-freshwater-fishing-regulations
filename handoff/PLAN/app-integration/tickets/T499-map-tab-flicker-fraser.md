# T499 — Map tab in the rules pane flickers, left open on the lowest Fraser stretch

Status: OPEN — owner 2026-11-08 (phone, 0.6.9). Part of T495 (smooth map).

With the pane's Map tab left open on the lowest Fraser stretch, the map flickered now and then with no input.

## Do
- Reproduce on the emulator (leave it open several minutes; log renders, camera and style/filter/feature-state writes).
- Find what rewrites the map while idle (timer, feed refresh, tile reload, re-render loop) and stop it.
