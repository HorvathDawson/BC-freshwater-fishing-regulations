# T1151 — Simplify small braids and side channels in the atlas so they stop becoming parts; one rule for where a channel joins

Status: OPEN — a later release; scoping and pictures first (owner 2026-11-25)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1151 | Simplify small braids and side channels in the atlas so they stop becoming parts; one rule for where a channel joins | OPEN — a later release; scoping and pictures first (owner 2026-11-25) | — | planning chat: a read-only scoping pass with pictures and the options; the owner picks; 0.10.0 ships T1140's "touching a water body" rule as a proposal meanwhile |

Source: owner 2026-11-25 evening, to the 0.10.0 chat on Halfway River p2 (three short channels 344–511 m from the
centre line because they meet the river's polygon or a lake on it), relayed verbatim: "We need a consistent way to
handle this and maybe the stream should be simplified and have some of the braidi g removed? Especially at the
mouth? Seems like our streams are too complicated and we could avoid this if small braids get simplified more. But
either way there should be a consistent way to handle this? Like enter lake having different rules or similar?
Thoughts?" And on the wordings: "maybe we could make the sections split more cleanly? Also simplification might
help reduce the many side channels". Related: T1140 (names), T1146 (side channels go with their river), T1149,
the memory notes `braids-paused`, `braid-edge-direction-defect`, `stretches-auto-combine`.

## Work (scoping first, read-only)
1. Count: side-channel and braid sections by length class and by where they sit (at a mouth, on a river polygon,
   into a lake); how many parts exist only because of one; the 26 nameless parts of T1140 as the worked cases.
2. Options with pictures and cost: merge braids under a length or width into the main stem in the atlas; treat a
   channel that lies on its river's polygon as the river; cleaner section splits; display-only simplification
   (the data keeps the lines, the app joins them). For each: what it does to rule bindings and to the map.
3. The consistent rule for where a channel joins (the proposal 0.10.0 ships in T1140: a channel whose end touches
   a water body of its own river joins there; a named lake names the part; the river's polygon or an unnamed lake
   gives the distance at the nearest point): confirm or replace it.
4. Build what he picks; diff every rule binding; tiles and bundle of one vintage.

## Acceptance
Set with his pick. At least: no part exists only because of a braid under the agreed size, and a build-time check
says so. Severity: medium. Model: Opus. Size: M for the scoping; the build is L (atlas). Owner: the pick.

## Owner

- [ ] 2026-11-25 — After the scoping pass: pick how braids and small side channels are simplified (options with pictures come then), and confirm or change the "channel joins where it touches its river's water body" rule that 0.10.0 ships as a proposal. Default until then: the proposal stands.
