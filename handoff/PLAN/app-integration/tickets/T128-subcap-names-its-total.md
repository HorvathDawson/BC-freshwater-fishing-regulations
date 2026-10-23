# T128 — A sub-cap line names the total it is part of ("of your 5 trout/char a day in streams…")

Status: PARTIAL — 5724e9c5, review fix b6376182 (own rule of only some fish: "Of the 3 a day under this lake’s rule", `tree.total_fish`; +46 strings) (lane wf_754f5fcd-d71-2). Built: `tree.total_named` + `tree.total_words` (one place), the
gate proves the named number, app `kindWords` bolds the kind's number; decision recorded in decisions.md ("CLAUDE
2026-10-22 (T128 …)"). Answers diff (scratch, full set): 392 distinct strings changed at 21,848 card positions, all
kind `says`/`sentence` now led by "Of your N … a day …"; card gate 0 problems on 7,180 cards. Left: the deliver
(answers rebuild) and, after it, re-cut the golden/parity fixtures (app/design/regs-v11/port/golden, ui-native
fixtures) — the live store still carries the old words, so parity passes today. Notes: no Region 4 card has a
hatchery sub-cap (pinned on Region 2 and Region 1 streams instead); no live group has a total limited to one kind of
water (z8 r5 is a one-kind row without a group, T80) — that branch is pinned on the fixture.

Owner questions (defaults built):
- T128-Qa: a kind under a water's own root total at the same number ("This creek’s rule: up to 1 a day per angler" →
  "Of your 1 trout and char a day, up to 1 may be rainbow trout") — keep, or name nothing there? Default: keep.
- T128-Qb: bump the rows section version 32 → 33 for the wording? Default: no (frame shape unchanged).

## Owner
"in the reg page we need to change the wording a little bit. instead of something like 'Up to 2 may be hatchery brook,
brown or cutthroat trout' it needs to add 'of your total X daily quota in a stream for trout and char' in there to make
it more obvious to an untrained user."

## Today
A kind row in the card's tree (`says`, answers ROUND18/19, pipeline/deliver/answers — card/rows wording; the schema text
is in app/packages/core/src/answers.generated.json) reads "Up to 2 may be hatchery brook, brown or cutthroat trout".
The `sentence`/`of` form ("Of the 2 a day: …") exists for the nearest count tier, but the line a reader sees first does
not say WHICH total it is part of, or that it is per day, or that it applies to streams. An untrained reader can take
"up to 2" as an extra 2 on top of the quota.

## Want
The line names its total, its unit and its water kind, in plain words, e.g.:
"Of your 5 trout and char a day in streams, up to 2 may be hatchery brook, brown or cutthroat trout."
- The total's number, the period ("a day"), the group's name ("trout and char") and the water kind ("in streams" /
  "in lakes", only when the total is limited to one) all come from the group the row belongs to — pipeline-generated,
  never assembled in the app.
- Same rule for every sub-cap line (count tiers, size bands, hatchery-only), lakes and streams, all regions.
- The headline and the tree must not repeat each other word for word — decide where the total is named once
  (decision recorded in decisions.md).

## Acceptance
- Whole-string tests on real cards: a Region 4 stream with the hatchery sub-cap, a lake total, a zone total limited to
  streams (z8 trout_char_quota.r5 shape), a one-kind group (T80), Kootenay Lake — Main Body.
- The card gate still proves every number on the reworded lines.
- Answers diff: list every changed `says`/`sentence` before shipping; golden/parity fixtures re-cut.
- Needs deliver (answers) + the app's Keep tree reads the new words (no logic in components).
