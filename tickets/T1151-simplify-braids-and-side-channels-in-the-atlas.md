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

## Scoped 2026-11-25 night (read-only; counts from candidate r-71d09ad82e81, before the T1146 fix)
Notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `T1151-scoping.md`.
- **Braids are not why waters have extra parts.** Named rivers have 15,782 side-channel groups and 95% already
  sit in the same part as the river beside them. Only 29 parts are side channel alone (T1140's 26 plus 3 named);
  about 22 stay after T1146.
- Those parts come from RULES, not size: an area judged by overlap (T1149), a river that is a region or unit
  line, a canal piece the walk will not enter, a licensing or salmon cut placed differently on the braid, and
  real closures. Deleting by size would delete closures: the Gordon's closed national-park channel is 46 m.
- The map clutter is a LOOK problem: the Halfway River has 299 side-channel sections against 16 on its main
  line; side channels are 1.2% of the stream tile bytes, so tile size is no reason to act.
- The join rule 0.10.0 ships (T1140, a proposal) names parts but breaks on channels that leave and never return
  (MacMillan, Theodosia), and it lives only in the naming step while rule binding uses a different notion.
- Recommendation: compute ONE join point per channel in the atlas; a channel takes its river's rules there unless
  an area covers it or a rule names it (only after T1149); improve the look in the app (fold and thin side
  channels by zoom), never by deleting lines.

## Owner

- [ ] 2026-11-25 — After the scoping pass: pick how braids and small side channels are simplified (options with pictures come then), and confirm or change the "channel joins where it touches its river's water body" rule that 0.10.0 ships as a proposal. Default until then: the proposal stands.
- [x] 2026-11-25 — Picks from the scoping (defaults first): a channel takes its river's rules / small braids are deleted / stay parts; a canal piece goes with its river / stays a part; a channel on a region-line river follows the river / keeps its own side; the look: app-only fold and hide / also a prune trial / nothing; the join rule: plus "joins where it leaves" / as shipped / no distance. → "Yes, the recommended set" (2026-11-26 00:50; the side-channel work of 2026-11-26 is T1176)
