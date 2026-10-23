# T128 — A sub-cap line names the total it is part of ("of your 5 trout/char a day in streams…")

Status: OPEN — owner 2026-10-22 (band 2: wording a reader can misread).

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
