# T174 — The remaining "answered → grey → answered" gaps on main rivers (after T169)

Status: OPEN

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
