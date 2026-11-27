# T1245 — "Eco Reserve" follow-ups from the #312 round 5 review

Status: OPEN — words only; none of them shows on today's data

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1245 | "Eco Reserve" follow-ups from the #312 round 5 review | OPEN — words only, none on today's data | — | picker tails and cut_words through eco_reserve; the case rule; the stop's remedy; the app fixtures at the next re-cut; owner answered 2026-11-26 (names only) |

Source: the independent re-review of PR #312 round 5 (T1172, head a91dc18c6), and the owner's ruling of 2026-11-26:
"it should be \"[name] eco reserve\" everywhere".

## Work
1. **Picker tails.** The picker's " · plus / without <rule what>" tail (`pipeline/deliver/answers/display/part.py:833-838`,
   `part_labels`) never goes through `eco_reserve`.
   - `cut_words` cuts before `eco_reserve` runs (`part.py:102`, the label's base at 130 characters; `part.py:118`, the
     "also" tail at 70 characters).
   - Fix: apply `eco_reserve` to `part_labels`' final output, or before `cut_words`. Test both.
2. **Case.** `part.py:250` (`eco_reserve`) does not keep the case.
   - A lowercased rule "what" gets "the Eco Reserve" mid-sentence.
   - An upper-case identifier gets "X Eco Reserve".
   - Decide the rule and test it.
3. **App fixtures still in the long form.** `app/packages/core/src/ends.parity.json` (lines 28, 34, 37, 78, 103, 109),
   the app tests that pin it (`regs/header.test.ts`, `waterNav.test.ts`, `waterNav.places.test.ts`, and
   `app/conformance/src/regs/waterGroup.fixture.test.ts` on fixture r-fc17c4373686) and `app/fixtures/golden/states.json`
   still read "Ecological Reserve". Refresh them at the next fixture re-cut.
4. **The stop's remedy.** The stop message of `builders.untold_side_parts`
   (`pipeline/deliver/answers/display/builders.py:416`, message at :426) should say what to do: "give the channel a
   `section_join`, or look at it".

## Acceptance
Tests for 1, 2 and 4, each failing without its fix. The fixtures in 3 carry the short form after the next re-cut.
Depends: none. Size: S. Owner: yes (below).

## Owner

- [x] 2026-11-26 — Where "Ecological Reserve" still shows on screen. ANSWERED 2026-11-26 (about 23:50 MST): "Merge; names only (Recommended)". Reading: #312 merged as built (the short "Eco Reserve" in names only); the full official name stays on the map's area labels and area tap, the water page's notes, the rules' own sentences and the salmon card's book lines.
