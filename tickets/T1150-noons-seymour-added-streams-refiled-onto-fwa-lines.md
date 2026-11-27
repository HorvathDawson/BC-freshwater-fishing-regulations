# T1150 — Added city streams that drain to the sea are filed under the nearest FWA river (Noons, Seymour, Squatters): own code, own water

Status: OPEN — the hardening release, all 11 lines, scoped then (owner 2026-11-25 evening); out of 0.10.0

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1150 | Added city streams that drain to the sea are filed under the nearest FWA river (Noons, Seymour, Squatters): own code, own water | OPEN — the hardening release, all 11 lines, scoped then (owner 2026-11-25 evening); out of 0.10.0 | — | Turner, Tunnel, Capitol done in T1246; the hardening release: the other 7 shared codes, the regenerated curated file, the registry id resync, his two leave-out rows; 0.10.0 ships only T1140's stopgap name (a proposal) |

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


## 2026-11-25 19:55: out of 0.10.0; all 11 go to the hardening release
The refile lane stopped before any commit: the fix cannot be limited to three lines. ELEVEN of the 36 city lines
that drain to the sea share an FWA line's exact code: 3 filed into another named water (Tunnel Creek in the
Seymour, Turner Creek in Noons Creek, Capitol Creek in the "Squatters Creek" item); 3 the other way round (FWA
lines sitting under the city names "Kask's Camp Creek", "Village Creek", "Mamquam Blind Channel Wetland"); 4 with
the same code but still separate items (Kyle Creek / School House Brook, Wilkes Creek / Hett Creek, Aliceville
Creek, Rainbow Creek); and Sheldens Creek, shared on purpose. The codes live in the stored curated file
`added_streams.json` (the generator reruns byte-identical), so any fix is a regenerated curated file: the plain
fix changes 83 of 320 rows (every later coastal code shifts), a no-cascade variant 31 rows; either moves the ids
of 5 registry waters (marks and matches naming them need a resync; memory note `registry-change-entry-resync`) and
makes the owner's two accepted "leave out" rows stale, which stops the reach run.

Owner, to the 0.10.0 chat (relayed), verbatim. Scope: "just add all to hardening release". His two rows: "do
whatever makes our current state decent until this is fixed in hardening".

So: nothing curated was written, his rows are untouched, no atlas rebuild in 0.10.0. The hardening release scopes
all 11 first (the row diff of each variant, the registry resync, what happens to his two rows: his call), then
builds. His wishes stand: own FWA-style code, in the graph all the way down; name variants and OpenStreetMap
checked; the build-time check that refuses an added stream filed under a river it does not touch. The lane's
notes (refile-finding.md, the two row diffs, the unfinished diff) are in the 0.10.0 chat's scratchpad `l30/`; that
chat keeps them or attaches them to its restart page. Stopgap in 0.10.0 (T1140, a proposal for his morning look):
a part made only of added city lines that carry their own name reads that name (Noons p4 as Turner Creek, Seymour
p3 as Tunnel Creek), with no distance and no "side channel".

## Scoped 2026-11-25 night (read-only)
Notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `T1150-scoping.md` (the earlier lane's row diffs are copied there).
- The finding holds and is complete: exactly 11 lines (12 rows) share an FWA code; none elsewhere in B.C., none
  among the non-coastal rows. Two holes: `estimate_coastal_wsc` (`coastal.py:46`) and `_bump_wsc`
  (`build_dataset.py:823`) check only city codes.
- **No rule is wrong today**: every affected line shows only the regional base rules. The defect is the wrong
  water: extra parts on the Seymour and Noons, searches that open a neighbouring water, "Schoolhouse Creek North"
  as 10 separate items.
- His name-variant question: none of the 11 is a local name for the FWA river it was filed under. OpenStreetMap
  (one read-only query) names Turner, Village, Wilkes and Rainbow creeks on the city lines themselves; a 2012
  Burnaby council report proposes the name Tunnel Creek. One open name: FWA line 360767895 is "Logger's Lane
  Creek" to the city and partly "Mamquam Blind Channel" in OpenStreetMap.
- Item ids follow the code, so codes must not cascade: the plain bump re-points 33 ids, the no-cascade draft 6
  (and wrongly moves Sheldens Creek). **Recommended third variant**: no cascade, Sheldens exempt, earlier codes
  kept, no old code reused: about 29 rows change and 3 ids re-point, all unavoidable. Designed, NOT run: the
  numbers are estimates.
- Found beside it: the public slug map has 11 creek URLs pointing at a neighbour since a code cascade on
  2026-10-29; an owner-reviewed Aliceville continuation has been stale (held) since then.
- The build check belongs at `atlas/build.py:559` with a twin in the generator; simulated on today's file it
  refuses exactly the 11 lines and passes Sheldens.

## 2026-11-27: Turner, Tunnel and Capitol creeks done in T1246
The owner ("All three now, in the re-cut"): T1246 gave the three codes of their own by hand (Turner 900-057238,
Tunnel 900-078371, Capitol 900-053686; tributaries under them), removed his Noons leave-out row, and closed the hole:
the coastal estimate skips FWA codes, and the atlas build and the generator refuse a city line on an FWA code by name
(`added_streams/validate.py` `fwa_code_collisions`). Left here: the seven other lines, named in
`FWA_CODE_SHARED_PENDING` (Kask's Camp, Village, Mamquam Blind Channel Wetland, Kyle, Wilkes, Aliceville, Rainbow);
"keep earlier codes" (a regeneration still re-mints every coastal code in order, so it would move hand-set codes);
the Seymour leave-out row (T1246's owner line).

## Owner

- [x] 2026-11-25 — Noons p4 and Seymour p3 cannot be named (misfiled added streams): what to do → "We want them to be refiled properly so let's take the fwa version for now but add a gotcha to check on which one is the real stream line." (2026-11-25)
- [x] 2026-11-25 — FWA has no line for them (real city creeks: Turner, Tunnel; Capitol the third): delete, or fix the cause → "Fix the cause but maybe they are name variants too. Maybe we should compare with osm too ? I'm not sure. What do you think is the right idea here. Either way the added streams on top of fwa need to get fwa code and stuff still and be part of the graph all the way down" (2026-11-25)
- [ ] 2026-11-25 — Hardening release, not blocking: check which line is the real stream and whether Turner, Tunnel and Capitol creeks are local name variants of an FWA line (compared with OpenStreetMap); the before/after map pictures of the three new waters come with the 0.10.0 morning summary. Default: they stay waters of their own.
- [x] 2026-11-25 — The fix touches 11 lines and regenerates a curated file: scope → "just add all to hardening release" (2026-11-25)
- [x] 2026-11-25 — Your two accepted "leave out" rows would go stale → "do whatever makes our current state decent until this is fixed in hardening" (2026-11-25)
- [ ] 2026-11-25 — Hardening release: after the scoping pass, pick the variant (83 rows change, or 31 with no cascade) and say what happens to your two "leave out" rows. Default: the no-cascade variant; your rows are kept until you say.
- [ ] 2026-11-25 — Picks for the hardening release (defaults first): the third variant / no-cascade / plain bump; your two "leave out" rows: remove in the same change / keep; Kyle, Wilkes, Aliceville and Rainbow: fix too / leave; line 360767895: look first / Logger's Lane Creek / Mamquam Blind Channel; the stale Aliceville continuation: re-key / leave held.
- [x] 2026-11-26 — The city streams in the re-cut → "Later, not in the re-cut" (2026-11-26 00:54)
