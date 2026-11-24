# T1049 — A start link's scroll lands short: it jumps to the first measured section top and never re-checks

Status: OPEN — 0.9.0 batch.

Source: independent review of #80 (T1038), 2026-11-23; on main too. On staging the Keep tab grows 903 → 1104 px about
35 ms after the first layout, so a start's jump (`RegsScreen`, polled until the tab's section has a top, then once) lands
on the old top: `tab=licence` showed Methods (1818 px), `tab=gear` landed short on a cold load (937 vs 1139). The same
code serves the web start link and the phone's link. Also: a link to a water's Map or Conditions view kept its
tab / sheet / scroll as a start that waited for the next Regulations page and landed there unasked.

## Work
1. The start's jump follows its section while the page settles: made again while the target moves, done once it has held
   (two 100 ms ticks), at most ~2 s. Web and phone (one component).
2. A start on another view than Regulations is dropped, and its keys are said (`dropped`, `reportDropped`), never kept to
   land later.
3. Tests that fail on main first, proved by mutation; web check; device check on the 0.9.0 device pass.

Depends: none (T1038 #80 touches the same effect: whichever lands second is rebased). Size: S. Owner: no.
