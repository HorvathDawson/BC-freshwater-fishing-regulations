# T797 — Printed rules the page never shows: "No angling from powered boats" (7 rules), dead fin fish as sturgeon bait, the Shuswap pier exemptions, "wild" on the Pitt Lake caveat

Status: PARTIAL (item 1 merged 7c077fdef (e39bcfed6); items 2 and 4 landed 45fff5e63 (8d7f17709); item 2's one-`says`-per-allowance 6c82b6d53, both allowances shown per the owner (1d3789b08); item 3, the Shuswap pier exemption, fixed as T789 K8 (dbaa009b9); left (Claude): item 5 (size sub-limit hint on other-fish rows, the Cowichan boats line's "on parts", the Halfway and Stellako part labels))

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T797 | Printed rules the page never shows: "No angling from powered boats" (7 rules), dead fin fish as sturgeon bait, the Shuswap pier exemptions, "wild" on the Pitt Lake caveat | PARTIAL (items 1-4 landed: 7c077fdef, 45fff5e63, 6c82b6d53, dbaa009b9 (T789 K8); left (Claude): item 5) | e39bcfed6, 8d7f17709, 6c82b6d53 | general audit 2026-11-16, answers checker D1, B2, B3, B4, E5, E6 (`handoff/PLAN/audit-2026-11-16/answers.md`) |

Source: general audit 2026-11-16, answers checker D1, B2, B3, B4, E5, E6 (`handoff/PLAN/audit-2026-11-16/answers.md`).
Owner 2026-11-16 go-ahead card: "Go on all (Recommended)". 2026-11-17 ~15:52, owner on item 2: "Show it (Recommended)" (the
dead-fin-fish-for-sturgeon allowance shows on sturgeon waters; reverses D18 a). 2026-11-17 ~16:30, item 2 follow-up (the
set-lining allowance in the same `also_allowed` field), verbatim label: "Show both (Recommended)": every allowance the book
prints under a bait ban shows on its waters.

Done:
1. "No angling from powered boats" (7 rules): `core/src/regs/boats.ts` `inBoatRules` selects `BOAT_WHILE` (`in_boat`,
   `in_powered_boat`; `OTHER_WHILE` = alone_in_boat, ice_fishing, set_lining); 47 parts of 35 waters gained the line; a
   province-wide walk in `conformance/src/regs/t670.release.test.ts` fails on a token no list knows (7c077fdef, e39bcfed6).
2. Dead fin fish as sturgeon bait: `core/src/regs/bait.ts` says each stored `also_allowed` (`decisions.md` D18 updated);
   one `says` per allowance (6c82b6d53), both allowances shown (1d3789b08) (45fff5e63, 8d7f17709).
3. Shuswap pier exemption: T789 K8 (dbaa009b9).
4. Pitt Lake head: `core/src/regs/caveats.ts` `inPartItem` adds `parts.conditions`: "Trout and char — release all, wild only".

## Work (left)
5. Smaller: other-fish rows with a size sub-limit (Region 4 kokanee "no more than 5 over 30 cm"; Zone B pike and
   walleye) show no hint on the row (it is one tap away; E5, the app's row words); the Cowichan boats line drops "on parts"
   from the speed limit (E6, the app's row words); part labels on the Halfway and Stellako (E7, not traced).
- For the main chat (data): the Nicomekl / Serpentine powered-boat rule reaches 27 side streams (Anderson Creek,
  Cloverdale Canal, …) with the words "upstream of dyke gates"; worth a look whether the book means the two rivers only.
- A "What's new" line with the next app version (item 1).
