# T1086 — RULINGS RU-14 (the Meziadin's row stops at Meziadin Lake) is outdated by T1053's "climb through every head lake"

Status: DONE (506132e69, lane L21, in T1053's PR): RU-14 itself still holds (it is the NASS stretch rule bounded at Meziadin Lake); its one outdated aside is rewritten.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1086 | RULINGS RU-14 (the Meziadin's row stops at Meziadin Lake) is outdated by T1053's head-lake walk | DONE — rewritten in T1053's PR (lane L21) | 506132e69 | 0.9.1 chat 2026-11-24 |

Source: the 0.9.1 chat's T1053 lane, 2026-11-24. The owner ruled on T1053: "Climb through every head lake" (all 36, also where the lake has a row of its own). RU-14 says the opposite for the Meziadin. Evidence: the T1053 lane's before/after list, `head-lakes-before-after.md` in the T1053-shots folder on branch feat/T1053-walk-climbs-head-lakes (not pushed when reported).

## Work
1. Rewrite RU-14 to the new ruling (or retire it, naming T1053), in the same PR as T1053's walk change so the rulings and the build never disagree on main.
2. Check the other rulings for the same assumption (a walk that stops at a lake).

## Acceptance
no ruling contradicts the built walk; the Meziadin's before/after rows are in T1053's list the owner reads.

Severity: medium (a written ruling the build now breaks). Release: 0.9.1, with T1053. Model: Opus. Size: S. Owner: no (the ruling is his T1053 answer).

## Done (lane L21, 2026-11-24)

RU-14 (`build._expander`, RULINGS T9's example, `test_phase3_rulings.py::test_the_nass_closure_takes_the_meziadin_to_its_lake_and_no_further`)
is about the NASS's stretch rule "from signs below the Meziadin confluence up to the Hwy 37 bridge": it takes the Meziadin
stem up to Meziadin Lake and none of the lake's creeks. That stays true: T1053 climbs only rules bound to their WHOLE
water, never a confluence stem (`build.climbs_head_lakes`, `_expander`: `climb_from` is the rule's own reach). The one
outdated sentence was the aside "which the Meziadin's own row stops at too": the Meziadin River's own ✱ row is a
whole-water rule and now climbs through Meziadin Lake (T18). Rewritten in `pipeline/atlas/reach/build.py` and in T9's
example in RULINGS; `tributaries.A_LAKE_ROWS_UNROWED_THROUGH_RIVER_IS_A_TRIBUTARY`'s note now says a stretch's top lake
still ends it and a whole water's is climbed. Other rulings checked (T1-T18, Z5, L-, Li5): none else says a walk stops at
a lake (Li5's Nanika example is licensing's walk DOWN, unchanged).
