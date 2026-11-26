# T1150 — Added city streams that drain to the sea are filed under the nearest FWA river (Noons, Seymour, Squatters): own code, own water

Status: OPEN — 0.10.0 (owner 2026-11-25); the cause is found (code); a fix + an atlas rebuild

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1150 | Added city streams that drain to the sea are filed under the nearest FWA river (Noons, Seymour, Squatters): own code, own water | OPEN — 0.10.0 (owner 2026-11-25); the cause is found (code); a fix + an atlas rebuild | — | 0.10.0 chat: the bump checks FWA codes; side atlas parity + binding diff before the PR; the two parts are a hard stop in T1140's naming until this lands |

Source: the 0.10.0 chat's T1140 naming lane, 2026-11-25. Noons Creek p4 and Seymour River p3 are added municipal
stream lines filed under those rivers but lying 693 m and 1,068 m from them, touching other creeks; they have no
flow link, so no join distance can be derived and the naming step stops on them (no silent label). Related: T1140,
T1146, the memory notes `added-streams-topology-resolver`, `added-streams-dem-orientation`.

Owner, 2026-11-25 evening (to the 0.10.0 chat, relayed), verbatim: "We want them to be refiled properly so let's
take the fwa version for now but add a gotcha to check on which one is the real stream line."

## Work
1. Read-only finding: what the two rows are, why they sit in those rivers' items, what FWA holds there, where the
   earlier "leave out" acceptance is recorded and why it did not take effect, and whether other added streams are
   misfiled the same way (a count, with the distance from their river).
2. For these two: stop using the added municipal line and use FWA's own line. This is a curated change: it is
   shown to the owner as a proposal with the before/after map picture; no agent writes a mark.
3. A gotcha where gotchas live (`pipeline/docs`), in his words: check which one is the real stream line.
4. Atlas rebuild; the expected parity stated before the run; diff every rule binding; tiles and bundle of one
   vintage.

## Acceptance
Neither water has a part that cannot be named; the binding diff is attributed line by line and touches only these
two waters (anything else stops and comes to the planning chat); a check refuses an added stream farther than the
snap limit from the river it is filed under. Severity: medium (a stream line shown under the wrong river).
Model: Opus. Size: S–M + one atlas rebuild. Owner: the before/after look; the gotcha's check later.


## 2026-11-25 19:46: the finding changes the fix (own code, own water; not "FWA's line")
The 0.10.0 chat's read-only finding: Noons p4 and Seymour p3 are real city creeks, Turner Creek (Port Moody,
2,198 m, with Turner Creek Trib.1 draining into it) and Tunnel Creek (Burnaby, 216 m). FWA has NO line for
either, so "take the FWA version" could only mean deleting them. Cause: a city line that drains to the sea is given
the watershed code of the nearest FWA river (`added_streams/coastal.py` `estimate_coastal_wsc`), and the
uniqueness bump checks only other city lines, not FWA codes: Tunnel Creek got the Seymour's exact code, Turner
Creek got Noons Creek's; the atlas then names a code-equal unnamed piece after the river and folds it into the
river's item (Seymour also answers to "Tunnel Creek", Noons to "Turner Creek"). A THIRD creek has the same fault:
Capitol Creek (Burnaby), filed as "Squatters Creek". The owner's earlier "leave out" is two accepted rows in
`unplaced_piece_calls.json`, read only by the reach's unplaced-piece gate: the rule skips the piece but the piece
stays in the item. Side find: a city "West Noons Creek" name sits on the unnamed FWA line 360244416, not on FWA's
gazetted West Noons Creek.

Owner, asked directly by the 0.10.0 chat (relayed), verbatim: "Fix the cause but maybe they are name variants too.
Maybe we should compare with osm too ? I'm not sure. What do you think is the right idea here. Either way the added
streams on top of fwa need to get fwa code and stuff still and be part of the graph all the way down".

The fix now (0.10.0): the bump also checks FWA codes, so the three creeks get their own FWA-style code, stay in the
graph down to the sea and become waters of their own with their names; Noons p4 and Seymour p3 disappear; nothing
is deleted. Plus the build-time check (an added stream beyond the snap limit from the river it is filed under is
refused by name) and a `GOTCHAS.md` entry. Proof before any PR: a side atlas build and parity (expected: three new
items; the Seymour, Noons and Squatters items lose exactly those sections and names; nothing else), then a side
reach run whose binding diff touches only those waters; anything else stops. If an owner-authored row must change
(the two accepted leave-out rows may go stale), the lane stops and the owner is asked. Work step 2 above ("use
FWA's own line") is replaced by this. For the hardening release (the open owner line below): are the three names
local variants of an FWA line (West Noons Creek 360595504 at 261 m; the unnamed 360548495 at 433 m), compared
with OpenStreetMap (read only; no agent uploads to OpenStreetMap, T1129).

## Owner

- [x] 2026-11-25 — Noons p4 and Seymour p3 cannot be named (misfiled added streams): what to do → "We want them to be refiled properly so let's take the fwa version for now but add a gotcha to check on which one is the real stream line." (2026-11-25)
- [x] 2026-11-25 — FWA has no line for them (real city creeks: Turner, Tunnel; Capitol the third): delete, or fix the cause → "Fix the cause but maybe they are name variants too. Maybe we should compare with osm too ? I'm not sure. What do you think is the right idea here. Either way the added streams on top of fwa need to get fwa code and stuff still and be part of the graph all the way down" (2026-11-25)
- [ ] 2026-11-25 — Hardening release, not blocking: check which line is the real stream and whether Turner, Tunnel and Capitol creeks are local name variants of an FWA line (compared with OpenStreetMap); the before/after map pictures of the three new waters come with the 0.10.0 morning summary. Default: they stay waters of their own.
