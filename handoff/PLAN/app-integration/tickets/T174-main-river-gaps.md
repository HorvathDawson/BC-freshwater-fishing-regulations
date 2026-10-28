# T174 — The remaining "answered → grey → answered" gaps on main rivers (after T169)

Status: DONE 2026-10-27 (Q153 + Q155 shipped in release r-bda3fea28838; Q154 measured, left off — owner)

Source: T169's census (2026-10-24). Inside side-channel waters the unexplained gaps are 0; outside them, these remain
(Flow / Level runs), each with a reason, but two classes are the owner's rule ("a predicted section flows into unpredicted
then back into predicted without reason") applied to ORDINARY river pieces, outside T169's narrow scope:
- 321 / 269: a main-river section left grey by its own walk while the side pieces adopted onto it answer past it.
- 84 / 89: a river's own side piece adopted onto a river section that has no answer (it takes the largest same-code
  section; Q169a: the nearest ANSWERED one instead?).
- Gravel Slough's 1425 connectors (3 sections; Q169c: lines take the answer of the polygons around them).
- A river's own unowned double-line polygon (Q169b: walk it as the river, as T101 does for stream items' polygons).
Not gaps to fix: 2,112 / 1,657 runs broken by a lake on the path; 4 area-ratio cases; 1 Belgo Creek confluence.

## Work
For each class: the cause in build_panels (why the main section's own walk is grey while its side pieces answer), one
rule that closes the gap without spreading predictions to non-braids (the owner's sanity rule, T169), a before/after
census and panel diff (sections that change are only those classes), whole-value tests. Ships: deliver. Size: M.

## As built — 2026-10-27 night (`pipeline/gauges/consume/panels.py`)
Owner questions answered by the recorded defaults (README "Owner questions"): Q155 "yes" → built and shipped; Q153 and
Q154 "measure first" → both built behind a switch and measured on the live atlas; Q153's measurement is pure gain on
side pieces only, so it ships (owner's T169 delegation: "do what you think is best just make sure it is sane"); Q154's
is not narrow, so it stays OFF for the owner.
- **Q153 `ADOPT_ANSWERED` (on).** `mainstem_candidates` = `mainstem_of`'s search returning every candidate, largest
  first (its first is exactly `mainstem_of`'s pick — pinned). A side piece whose largest same-code section in reach is
  silent adopts the largest one that answers; none answers → silent, as before.
- **Q155 `WATER_LINE_GAPS` (on).** `water_line_gaps`: a connected run of one stream water's unanswered LINES whose
  every answered neighbour of the same water, up and down, carries ONE answer (same panel and area; at least one each
  side) takes it. Another water is never read; two answers at the ends (a gauge between, a confluence) leave it.
  Runs after the polygon passes, before the side-channel water pass.
- **Q154 `RIVER_WIDENINGS` (OFF).** `river_widenings`: an unowned polygon a stream enters and leaves on the polygon's
  own code walked as the river. Finding: the atlas has NO double-line river polygons — these are FWA lakes
  (`WATERBODY_TYPE L`, mostly < 1 ha) the river threads. Measured ON: 96,590 sections change (+95,219 Flow, +51,172
  Level; 34,283 unowned lakes get a panel, 54,776 unnamed lines) — it is "a lake on the path no longer ends the
  relationship", province-wide, not a gap fix. Owner call (Q154 updated with the numbers).

Measured (panels rebuilt from the live atlas + bundle inputs, scratch `measure.py`; base reproduces the live
`section_panel` — 247,595 sections, same member sets; 370 differ only in member order from donor-list order):
| | sections w/ panel | Flow answered | Level answered | Flow runs | Level runs |
|---|---|---|---|---|---|
| live (base) | 247,595 | 246,840 | 128,310 | 2,473 | 1,984 |
| Q153 only | 247,743 | +148 | +115 | 2,471 | 1,979 |
| Q155 only | 247,914 | +317 | +261 | 2,174 | 1,758 |
| **shipped (Q153+Q155)** | **248,004** | **247,247 (+407)** | **128,637 (+327)** | **2,196** | **1,769** |
| + Q154 | 343,451 | +95,497 | +51,395 | 1,285 | 1,312 |
Shipped: 409 sections change, ALL previously unanswered (0 answers altered, 0 lost): 153 side pieces owned by a water,
28 unowned side pieces, 228 owned non-side lines. Most: Kitsumkalum 45+47, Swift, Elk, Dease, Cranberry, Lillooet,
Nahatlatch, Bowser, Moberly, Bull, Adams. Census classes (Flow, my classifier): adopted-onto-silent 59 → 3; a water's
own line between its pieces 229 → 47 (the rest: different answers at the ends); unowned-lake-on-path 1,761 (Q154);
true lake 329 → 276. Gravel Slough (gnis:8009): 3 connectors silent → 1 (356292469:8602, :8752 take Elk Creek's
08MF048/08MF018; :9077 sits between 08MF064 and Elk — two answers, a reason).

Ship: `pipeline.deliver all` → **r-bda3fea28838**. Binding diff: bundle tables vs the previous bundle byte-identical
except `section_panel` (+409 rows, 0 removed; `panel_member` unchanged) — no rule binding moves; release files differ
from r-f7faa483d2b1 only in `flow-area` and `paint`. Re-cut: regs-fixture (r-aac4ded772b9), golden sync at HEAD
unchanged (taps 126/126); conformance/regs 4193 pass.
Tests: `pipeline/tests/test_t174_river_gaps.py` (whole values, narrow cases, switch-off mutation pins, needs_bundle
Gravel 11/1); `test_stream_polygon_panels` (Nicomen miniature's `mid` now answers), `test_build_panels` (code-condition
pin follows the search into `mainstem_candidates`).
