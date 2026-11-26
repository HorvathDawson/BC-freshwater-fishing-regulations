# T585 — Web and phone answer from the same feed and release, and say which

Status: IN PROGRESS — F8, F12, F13, F14 built (5502ba714, 04316bd91, c58640b86, e4add6e9a); the owner accepted the new words 2026-11-12 (old owner to-do "Accept all as built": T585/T555 gauge wordings); the staging-only "Data release" line on the gauge card was removed by T920 (b49e10e49, owner-approved Conditions redesign). left (Claude): the emulator check under "Not device-tested" (cold-start "Updated to …" said once; offline kept-reading words).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T585 | Web and phone answer from the same feed and release, and say which | IN PROGRESS (F8, F12-F14 built; owner accepted the words 11-12; Data release line removed by T920; left (Claude): emulator check) | 5502ba714, 04316bd91, c58640b86, e4add6e9a | T580 F8, F12–F14; with T550; HYDAT-differs in T633 |

Source: T580 audit 2026-11-10 (owner: "stuff like this should never happen … look for other things like that").
Findings, with file:line, the state that shows each and the proposed fix: `handoff/PLAN/app-integration/T580-findings.md`.

## What
F8 (the card's figure and percentile from a different file than the rows), F12 (the phone on an older release than the
web), F13 (a switchable build keeping another environment / channel), F14 (offline "now" with no standing). Goes with
T550.

Done: F8 + F14 (5502ba714: `feed.now()` ranks only the reading the index row names, else `Reading.rankedAt`); F13
already pinned (env.test.ts); the card's words (04316bd91, c58640b86: `readingStanding`, kept words offline; owner
accepted 2026-11-12); F12 "as of" line (e4add6e9a, `dataProvenanceLine`; later removed by T920, b49e10e49); F12 cold
start (e4add6e9a: `Booted.applied`, `release/opened`, `bootSaid`; boot test `tools/release-boot.test.ts` "(T585 F12) …").
Not built: installing a newer set BEFORE the map on a cold start (it would hold the start for a download). HYDAT
differs: T633 item 5 (9995fcf13).

## Work (left) — the emulator check
- Stage an update, kill the app before the still moment, open it: "Updated to …" shows once, the Updates log has
  "applied", a second open says nothing. (Not run yet.)
- 2026-11-20 run (L4, build of 124d79efa): `gauge-offline` PASS (kept line, "Offline — measured line as of 2026-11-20 at
  08:35 PT, saved on this phone"); `bg-update` PASS 8/8 incl. "T585: a second return and a cold start do not say
  'Updated to' again". NOT proven: that the words showed once at the apply (no toast seen in 20 s: maybe not in the
  accessibility tree, or not shown; a screenshot at the apply would settle it); the Updates log had no "Release, applied"
  row at the second look.

Acceptance: each finding named above no longer reproduces, and the test or gate the findings file proposes for it is in.

Depends: T567. Size: M. Owner: no.
