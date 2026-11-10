# T592 — A saved place's map legend still lists "proposal (not applied)"

Status: DONE — 4089f60cd, 2026-11-10 — `checklist.showsProposal` (frontend test); not looked at in a browser on the Forks

Source: T532 round 9 (the Forks), leftover f.

## Today
On a place whose extent the owner saved (waiting for its cut), the map legend still shows "proposal (not applied)"
and draws our earlier proposal in purple under his saved extent (`CheckMap`: `loaded.proposal && !edit`).

## Work
Do not draw or list our proposal once the place holds his save (`check` saved / unapplied, or held) — the same rule
the panel's proposal block already uses. A pure helper + test.

Depends: —. Size: S. Owner: no.
