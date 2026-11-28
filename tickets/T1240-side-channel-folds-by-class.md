# T1240 — Side-channel parts fold into their river, by class (the 17 artefacts of the 23)

Status: OPEN — in the 0.10.0 re-cut; the owner chose to hold the re-cut for all of the folds

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1240 | Side-channel parts fold into their river, by class (the 17 artefacts of the 23) | OPEN — in the re-cut, which holds for the folds | — | five fix classes; Strathcona-Westmin ruled; Snehumption ruled |

Source: the owner's picks of 2026-11-26 (planning chat), verbatim: side channels, after the naming round 2: "yes to all
3 but it begs the question why the side channel has different regs and is not combined with neighbours if it is not
getting differen regs from an area or something else that could be in the naming?"; the water option "D: simplify the
water itself" (its parts C and D share: the folds); late folds: "Hold the re-cut for all of them"; Snehumption: first "i
need to see on map for how to handle it", then, after the map, "The whole creek is a Similkameen tributary" (after "Plain \"2 side channels\" (Recommended)" for its label). The notes:
`braids-design` (section 1), `naming-model-design` round 3 (the one agreed table), `water-options`. Related: T1176
(superseded), T1149, T1150, T1172, T1191, T1199.

Of the 23 side-channel-only parts, 6 are real (Gordon, Squamish, Oxbows, Old Kootenay, Theodosia "Drains to Powell
Lake", and Snehumption as the owner's pick) and 17 are artefacts of HOW a rule binds. Every separate part must be able
to say why it is separate; if nothing nameable differs, the split is suspect.

## Work: the fix classes
1. **A channel of a river on a unit line takes both units** (the river is in both, as Campbell River is): Cottonwood p1
   and West Road p3 have no Region 5 rules, and Campbell p6 lacks the MU 1-1..1-6 summer closure. These three give
   WRONG ANSWERS on the shipped release today (Campbell reads open Jul 15-Aug 31). First in line; it is option A's
   whole content.
2. **A park rule bound by overlap** (Strathcona: Burman, Bedwell, Rees, Comox, Thelwood): the cut of T1149; the channels
   fold into their river's part. In the 0.10.0 re-cut, by "Hold the re-cut for all of them" (2026-11-26) and "Yes, treat it
   as Strathcona Park (Recommended)" (2026-11-27); for Strathcona this replaces T1149's "others later" of 2026-11-25.
3. **The tributary walk stops at a canal or lake** (Mahood p1 and three more): a canal piece of a river goes with its
   river (`may_enter`).
4. **A licence or salmon cut placed differently on the braid** (Skeena p4, Klinaklini p1, MacMillan p2; the Peace's Site C
   stretch): the braid sweep carries the river's licence and salmon cuts to its channels. 46 channel groups (47 km) gain
   their river's closure; 29 of them (22 km) under option A. Skeena p4 is closed by the reserve either way.
5. **City streams filed on the wrong river** (Noons p4 refiled as Turner Creek; Seymour): T1150.

Every change gets a per-rule binding diff (AGENTS rule 63) and an answers diff. The water-options question was whether
folds that are not ready when the from-nothing rebuild starts (the Strathcona cut, the canal walk, the braid sweep) ship
later in 0.11.0 or hold the re-cut; his answer was "Hold the re-cut for all of them", so the re-cut waits for these folds.
The data and release chat tells him which one is late, and the loop-removal trial (T1242) is separate.

## Acceptance
Each of the 17 either folds (its water's parts list loses the part) or is listed with the reason it is separate; no
answer changes except the listed ones; the Campbell, Cottonwood and West Road answers are right on the new release.

Severity: medium (3 wrong answers today). Model: Opus. Size: L over the classes. Built by the data and release chat.

## Snehumption (ruled)
"The whole creek is a Similkameen tributary": the side channel folds into the main line, which opens the 17 km main line in spring. Built as a rule: a stream that leaves B.C. and joins a B.C. river downstream of the border is that river's tributary. The other waters the rule touches are listed and shown to him first. Province list entry 42 (the confluence is in the U.S.).

## Owner

- [x] 2026-11-26 — Strathcona-Westmin: does "Strathcona - Westmin Park" count as the Strathcona Park the rule names? Thelwood's channel is 100% in it, its main line 18%. Default: yes, the rule applies in Westmin.
  ANSWERED 2026-11-27, verbatim: "Yes, treat it as Strathcona Park (Recommended)".
