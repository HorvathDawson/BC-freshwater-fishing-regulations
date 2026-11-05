# T396 — Trout & more legend takes three lines; switching modes jumps

Status: OPEN (owner report 2026-11-05, 0.5.0 build 8, map).

The Trout & more legend wraps to 3 lines where 2 should fit, and because the Salmon legend is a different height the
bottom bar jumps when switching between Salmon and Trout & more. Fit it in 2 lines (phone width ~390 dp, the T283 text
size) and give both modes' legends the same height. Ties to T377 (tappable/collapsible legend).
