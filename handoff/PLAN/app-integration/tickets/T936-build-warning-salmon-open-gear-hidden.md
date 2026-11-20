# T936 — Build warning: a water whose salmon are open on a game-fish-closed day that carries a B.C. gear rule

Status: OPEN.

Source: owner 2026-11-20 ~01:03 (main chat), after T934. Asked whether a B.C. closure closes salmon: yes, a FULL closure
does (rule book p.77; T796 A5 puts it on the salmon chains). The leftover case: the game-fish strip reads closed on a day
by species closures that together close every game fish (no single full closure), DFO has salmon open or release that
day, and the B.C. gear answer holds a bait ban / fly fishing only / artificial flies only. The strip drops gear bits on
a closed day (`display/strip.py` `strip_code`: CLOSED alone), so the salmon bar shows no gear mark there (T934; owner:
"closed is only closed"). Verbatim: "Maybe add a build warning if there is a Waterboys [waterbody] like that".

## Work
In the release build (beside `bundle_problems` in `pipeline/deliver/store/build.py`, or where the salmon chains and the
display / gear frames are both at hand): list each water + section + day span where all three hold: game-fish display
status closed, not by a book full closure (T796); a DFO salmon verdict open / release; the gear answer at the same part
key and segment carries `bait_ban` or `fly`. Print it as a WARNING with a count and the first N rows (never fails the
build), and write the full list beside the release's other build reports. A test pins one synthetic case.
## Acceptance
The build prints the warning (or "none"); the count is reported to the owner with the next release.
Depends: none. Size: S. Owner: no (see the count with the release). Data change: rides the next data release.
