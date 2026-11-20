# T586 — Small one-answer fixes: saved spot lead, silent gauge dot, two clocks

Status: DONE — 5a2e4117b (closed on the owner's word 2026-11-20; see the last section)

Source: T580 audit 2026-11-10 (owner: "stuff like this should never happen … look for other things like that").
Findings, with file:line, the state that shows each and the proposed fix: `handoff/PLAN/app-integration/T580-findings.md`.

## Work
- F9: a saved spot freezes the Flow lead whatever the angler was looking at (check against the Flow-leads rule: may be correct now).
- F10 (owner): the browse map drops the dot of a station silent in the quantity, while the donor pin and row keep it and say why. ANSWERED owner 2026-11-10: "it should be muted for the gauge": keep the dot, muted.
- F15 (plausible): two clocks; goes with T551.

Acceptance: each finding named above no longer reproduces, and the test or gate the findings file proposes for it is in.

Depends: T567. Size: S. Owner: yes (F10).

## Closed 2026-11-20

Closed 2026-11-20 on the owner's word ("Couldn't we just assume most of these are done already?"): the saved-spot lead and the muted dot for a silent gauge are taken as checked on the phone.

Status line before closing: IN REVIEW — 5a2e4117b (F9: a saved spot captures `leadOver`; F10, owner "it should be muted for the gauge": a silent gauge keeps its dot, grey, labelled with the row's words, today only); F15 fixed with T551 item 1 (d3b7f080e; the clock gate has no exception left, T605). Proof: the owner confirms on the phone.
